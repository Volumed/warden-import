require("dotenv").config();

const axios = require("axios");
const mysql = require("mysql2/promise");
const amqp = require("amqplib/callback_api");

const username = process.env.RABBITMQ_USER;
const password = process.env.RABBITMQ_PASS;
const host = process.env.RABBITMQ_HOST;
const connectionString = `amqp://${username}:${password}@${host}`;

const webhookUrl = process.env.WEBHOOK_URL;
const webhookUrlManagement = process.env.WEBHOOK_MANAGEMENT_URL;

let connection = false;
let serverDoesntExist = false;
let totalUsers = { new: 0, updated: 0 };
let totalUsersChat = { new: 0, updated: 0 };

let _guildId;
let oldGuildId;
let scanDoneTimer = null;
let wasDone = false;

const messageQueueScraper = [];
const messageQueueChat = [];

const sendDiscordWebhookMessage = async (description, color = 0x00ff00) => {
	try {
		await axios.post(webhookUrl, {
			embeds: [
				{
					description: description,
					color: color,
				},
			],
		});
	} catch (error) {
		console.error("Error sending message:", error);
	}
};

const sendDiscordWebhookManagementMessage = async (
	description,
	color = 0x00ff00,
) => {
	try {
		await axios.post(webhookUrlManagement, {
			embeds: [
				{
					description: description,
					color: color,
				},
			],
		});
	} catch (error) {
		console.error("Error sending message:", error);
	}
};

const getConnection = async () => {
	if (!connection || connection.connection._closing) {
		connection = await mysql.createConnection({
			host: process.env.DB_HOST,
			user: process.env.DB_USER,
			password: process.env.DB_PASSWORD,
			database: process.env.DB_NAME,
		});
	}
	return connection;
};

const resetScanDoneTimer = () => {
	if (scanDoneTimer) {
		clearTimeout(scanDoneTimer);
	}
	scanDoneTimer = setTimeout(
		async () => {
			await sendDiscordWebhookMessage(
				`Import done for **${_guildId}**\n New users: **${totalUsers["new"]}**\n Updated users: **${totalUsers["updated"]}**`,
			);

			console.log("Guild done, resetting");

			serverDoesntExist = false;
			totalUsers = { new: 0, updated: 0 };
			wasDone = true;
		},
		1.5 * 60 * 1000,
	);
};

const guildIdHandler = {
	get: () => {
		return _guildId;
	},
	set: async (newValue) => {
		oldGuildId = _guildId;
		_guildId = newValue;

		resetScanDoneTimer();

		if (oldGuildId !== undefined && oldGuildId !== _guildId) {
			if (wasDone) {
				wasDone = false;
			} else {
				await sendDiscordWebhookMessage(
					`Import done for **${oldGuildId}**\n New users: **${totalUsers["new"]}**\n Updated users: **${totalUsers["updated"]}**`,
				);

				console.log("Guild done, resetting");

				serverDoesntExist = false;
				totalUsers = { new: 0, updated: 0 };
			}
		}
	},
};

Object.defineProperty(global, "guildId", guildIdHandler);

const processUserScraper = async (serverId, user) => {
	const typeHierarchy = ["OTHER", "LEAKER", "CHEATER", "SUPPORTER", "OWNER"];

	let added = 0;
	let updated = 0;

	const [serverRows] = await connection.execute(
		"SELECT id FROM BadServers WHERE id = ?",
		[serverId],
	);

	if (serverRows.length === 0) {
		await sendDiscordWebhookMessage(
			`${serverId} doesn't exist in the database`,
			0xff0000,
		);
		serverDoesntExist = true;
		return;
	}

	const { id, type, roles = [] } = user;

	if (!id || !type) {
		return;
	}

	const [rows] = await connection.execute(
		"SELECT id, type, status FROM Users WHERE id = ?",
		[id],
	);

	let status = "BLACKLISTED";
	if (type === "SUPPORTER" || type === "OWNER") {
		status = "PERM_BLACKLISTED";
	}

	if (rows.length > 0) {
		const currentType = rows[0].type;
		const currentStatus = rows[0].status;

		if (currentStatus === "WHITELISTED") {
			const rolesString = roles.length > 0 ? roles.join(", ") : "";
			await sendDiscordWebhookManagementMessage(
				`SCAN LOG: User <@${id}> was found in ${serverId}\n roles: ${rolesString}`,
			);
			console.log(`User ${id} is whitelisted, skipping.`);
			return;
		}

		if (currentStatus === "PERM_BLACKLISTED") {
			status = "PERM_BLACKLISTED";
		}

		const currentTypeIndex = typeHierarchy.indexOf(currentType);
		const newTypeIndex = typeHierarchy.indexOf(type);

		if (newTypeIndex > currentTypeIndex) {
			await connection.execute(
				"UPDATE Users SET type = ?, status = ? WHERE id = ?",
				[type, status, id],
			);
		} else {
			await connection.execute("UPDATE Users SET status = ? WHERE id = ?", [
				status,
				id,
			]);
		}
	} else {
		await connection.execute(
			"INSERT INTO Users (id, last_username, avatar, type, status) VALUES (?, ?, ?, ?, ?)",
			[
				id,
				"EMPTY",
				"https://cdn.discordapp.com/embed/avatars/0.png",
				type,
				status,
			],
		);
	}

	const rolesString = roles.length > 0 ? roles.join(", ") : "";

	const [importRows] = await connection.execute(
		"SELECT id, type FROM Imports WHERE id = ? AND server = ?",
		[id, serverId],
	);

	if (importRows.length > 0) {
		const currentType = importRows[0].type;
		const currentTypeIndex = typeHierarchy.indexOf(currentType);
		const newTypeIndex = typeHierarchy.indexOf(type);

		if (newTypeIndex > currentTypeIndex) {
			await connection.execute(
				"UPDATE Imports SET type = ?, roles = ?, updatedAt = NOW(), appealed = 0 WHERE id = ? AND server = ?",
				[type, rolesString, id, serverId],
			);
			updated = updated + 1;
		} else {
			await connection.execute(
				"UPDATE Imports SET roles = ?, appealed = 0, updatedAt = NOW() WHERE id = ? AND server = ?",
				[rolesString, id, serverId],
			);
			updated = updated + 1;
		}
	} else {
		await connection.execute(
			"INSERT INTO Imports (id, server, roles, type, appealed, createdAt, updatedAt, reason) VALUES (?, ?, ?, ?, ?, NOW(), NOW(), ?)",
			[id, serverId, rolesString, type, 0, ""],
		);
		added = added + 1;
	}

	totalUsers["new"] += added;
	totalUsers["updated"] += updated;
};

function convertServersTypeToUsersType(BadServersType) {
	return BadServersType.map((type) => {
		if (type === "CHEATING") {
			return "CHEATER";
		} else if (
			type === "RESELLING" ||
			type === "ADVERTISING" ||
			type === "OTHER"
		) {
			return "OTHER";
		} else if (type === "LEAKING") {
			return "LEAKER";
		}
		return type;
	});
}

const processUserChat = async (serverId, user) => {
	const typeHierarchy = ["OTHER", "LEAKER", "CHEATER", "SUPPORTER", "OWNER"];

	let added = 0;
	let updated = 0;

	const [serverRows] = await connection.execute(
		"SELECT id, type FROM BadServers WHERE id = ?",
		[serverId],
	);

	if (serverRows.length === 0) {
		console.error(`${serverId} doesn't exist in the database`);
		return;
	}

	user.type = convertServersTypeToUsersType([serverRows[0].type])[0];

	const { id, type, roles = [] } = user;

	if (!id || !type) {
		return;
	}

	const [rows] = await connection.execute(
		"SELECT id, type, status FROM Users WHERE id = ?",
		[id],
	);

	let status = "BLACKLISTED";
	if (type === "SUPPORTER" || type === "OWNER") {
		status = "PERM_BLACKLISTED";
	}

	if (rows.length > 0) {
		const currentType = rows[0].type;
		const currentStatus = rows[0].status;

		if (currentStatus === "WHITELISTED") {
			const rolesString = roles.length > 0 ? roles.join(", ") : "";
			await sendDiscordWebhookManagementMessage(
				`CHAT LOG: User <@${id}> was found in ${serverId}\n roles: ${rolesString}`,
			);
			console.log(`User ${id} is whitelisted, skipping.`);
			return;
		}

		if (currentStatus === "PERM_BLACKLISTED") {
			status = "PERM_BLACKLISTED";
		}

		const currentTypeIndex = typeHierarchy.indexOf(currentType);
		const newTypeIndex = typeHierarchy.indexOf(type);

		if (newTypeIndex > currentTypeIndex) {
			await connection.execute(
				"UPDATE Users SET type = ?, status = ? WHERE id = ?",
				[type, status, id],
			);
		} else {
			await connection.execute("UPDATE Users SET status = ? WHERE id = ?", [
				status,
				id,
			]);
		}
	} else {
		await connection.execute(
			"INSERT INTO Users (id, last_username, avatar, type, status) VALUES (?, ?, ?, ?, ?)",
			[
				id,
				"EMPTY",
				"https://cdn.discordapp.com/embed/avatars/0.png",
				type,
				status,
			],
		);
	}

	const rolesString = roles.length > 0 ? roles.join(", ") : "";

	const [importRows] = await connection.execute(
		"SELECT id, type FROM Imports WHERE id = ? AND server = ?",
		[id, serverId],
	);

	if (importRows.length > 0) {
		const currentType = importRows[0].type;
		const currentTypeIndex = typeHierarchy.indexOf(currentType);
		const newTypeIndex = typeHierarchy.indexOf(type);

		if (newTypeIndex > currentTypeIndex) {
			await connection.execute(
				"UPDATE Imports SET type = ?, roles = ?, updatedAt = NOW(), appealed = 0 WHERE id = ? AND server = ?",
				[type, rolesString, id, serverId],
			);
			updated = updated + 1;
		} else {
			await connection.execute(
				"UPDATE Imports SET roles = ?, appealed = 0, updatedAt = NOW() WHERE id = ? AND server = ?",
				[rolesString, id, serverId],
			);
			updated = updated + 1;
		}
	} else {
		await connection.execute(
			"INSERT INTO Imports (id, server, roles, type, appealed, createdAt, updatedAt, reason) VALUES (?, ?, ?, ?, ?, NOW(), NOW(), ?)",
			[id, serverId, rolesString, type, 0, ""],
		);
		added = added + 1;
	}

	totalUsersChat["new"] += added;
	totalUsersChat["updated"] += updated;
};

const sendTotalUsersChatEmbed = async () => {
	if (totalUsersChat.new > 0 || totalUsersChat.updated > 0)
		await sendDiscordWebhookMessage(
			`Total Users added from chat log: **${totalUsersChat["new"]}**\n Total Users updated from chat log: **${totalUsersChat["updated"]}**`,
		);
	totalUsersChat = { new: 0, updated: 0 };
};
setInterval(sendTotalUsersChatEmbed, 10 * 60 * 1000);

async function processQueueScraper() {
	while (true) {
		if (messageQueueScraper.length > 0) {
			const task = messageQueueScraper.shift();
			if (task) await task();
		} else {
			await new Promise((resolve) => setTimeout(resolve, 100));
		}
	}
}

async function processQueueChat() {
	while (true) {
		if (messageQueueChat.length > 0) {
			const task = messageQueueChat.shift();
			if (task) await task();
		} else {
			await new Promise((resolve) => setTimeout(resolve, 100));
		}
	}
}

amqp.connect(connectionString, (error0, connection) => {
	if (error0) {
		throw error0;
	}
	connection.createChannel(async (error1, channel) => {
		if (error1) {
			throw error1;
		}

		const queueScraper = "users";

		channel.assertQueue(queueScraper, {
			durable: false,
		});

		console.log("Waiting for users from web");
		await sendDiscordWebhookMessage(`Ready to import users from web`);

		connection = await getConnection().catch((err) => {
			return console.error(err);
		});

		channel.consume(
			queueScraper,
			async (msg) => {
				const item = JSON.parse(msg.content);
				guildId = item.guildId;
				messageQueueScraper.push(async () => {
					try {
						console.log("Scraper", item);
						if (item.id && !serverDoesntExist)
							await processUserScraper(item.guildId, item);
						await new Promise((resolve) => setTimeout(resolve, 500));
					} catch (error) {
						console.error(error);
					}
				});
			},
			{
				noAck: true,
			},
		);
		await processQueueScraper().catch(console.error);
	});
});

amqp.connect(connectionString, (error0, connection) => {
	if (error0) {
		throw error0;
	}
	connection.createChannel(async (error1, channel) => {
		if (error1) {
			throw error1;
		}

		const queueChat = "usersChat";

		channel.assertQueue(queueChat, {
			durable: false,
		});

		console.log("Waiting for users from chat");
		await sendDiscordWebhookMessage(`Ready to import users from chat`);

		connection = await getConnection().catch((err) => {
			return console.error(err);
		});

		channel.consume(
			queueChat,
			async (msg) => {
				const item = JSON.parse(msg.content);
				messageQueueChat.push(async () => {
					try {
						console.log("Chat", item);
						if (item.id) await processUserChat(item.guildId, item);
						await new Promise((resolve) => setTimeout(resolve, 500));
					} catch (error) {
						console.error(error);
					}
				});
			},
			{
				noAck: true,
			},
		);
		await processQueueChat().catch(console.error);
	});
});
