require('dotenv').config()

const axios = require('axios')
const mysql = require('mysql2/promise')
const amqp = require('amqplib/callback_api')

const username = process.env.RABBITMQ_USER
const password = process.env.RABBITMQ_PASS
const host = process.env.RABBITMQ_HOST
const connectionString = `amqps://${username}:${password}@${host}`

const webhookUrl = process.env.WEBHOOK_URL

let connection = false
let serverDoesntExist = false
let totalUsers = { new: 0, updated: 0 }

let _guildId
let oldGuildId
let inactivityTimer = null
let scanDoneTimer = null
let wasDone = false

const sendDiscordWebhookMessage = async (description, color = 0x00ff00) => {
	try {
		await axios.post(webhookUrl, {
			embeds: [
				{
					description: description,
					color: color,
				},
			],
		})
	} catch (error) {
		console.error('Error sending message:', error)
	}
}

const getConnection = async () => {
	if (!connection || connection.connection._closing) {
		connection = await mysql.createConnection({
			host: process.env.DB_HOST,
			user: process.env.DB_USER,
			password: process.env.DB_PASSWORD,
			database: process.env.DB_NAME,
		})
	}
	return connection
}

const resetScanDoneTimer = () => {
	if (scanDoneTimer) {
		clearTimeout(scanDoneTimer)
	}
	scanDoneTimer = setTimeout(async () => {
		await sendDiscordWebhookMessage(
			`Import done for **${_guildId}**\n New users: **${totalUsers['new']}**\n Updated users: **${totalUsers['updated']}**`
		)

		console.log('Guild done, resetting')

		serverDoesntExist = false
		totalUsers = { new: 0, updated: 0 }
		wasDone = true
	}, 1.5 * 60 * 1000)
}

const guildIdHandler = {
	get: () => {
		return _guildId
	},
	set: async (newValue) => {
		oldGuildId = _guildId
		_guildId = newValue

		resetScanDoneTimer()

		if (oldGuildId !== undefined && oldGuildId !== _guildId) {
			if (wasDone) {
				wasDone = false
			} else {
				await sendDiscordWebhookMessage(
					`Import done for **${oldGuildId}**\n New users: **${totalUsers['new']}**\n Updated users: **${totalUsers['updated']}**`
				)
	
				console.log('Guild done, resetting')
	
				serverDoesntExist = false
				totalUsers = { new: 0, updated: 0 }
			}
		}
	},
}

Object.defineProperty(global, 'guildId', guildIdHandler)

const processUser = async (serverId, user) => {
	const typeHierarchy = ['OTHER', 'LEAKER', 'CHEATER', 'SUPPORTER', 'OWNER']

	let added = 0
	let updated = 0

	const [serverRows] = await connection.execute(
		'SELECT id FROM BadServers WHERE id = ?',
		[serverId]
	)

	if (serverRows.length === 0) {
		await sendDiscordWebhookMessage(
			`${serverId} doesn't exist in the database`,
			0xff0000
		)
		serverDoesntExist = true
		return
	}

	const { id, type, roles = [] } = user

	if (!id || !type) {
		return
	}

	const [rows] = await connection.execute(
		'SELECT id, type, status FROM Users WHERE id = ?',
		[id]
	)

	let status = 'BLACKLISTED'
	if (type === 'SUPPORTER' || type === 'OWNER') {
		status = 'PERM_BLACKLISTED'
	}

	if (rows.length > 0) {
		const currentType = rows[0].type
		const currentStatus = rows[0].status

		if (currentStatus === 'WHITELISTED') {
			console.log(`User ${id} is whitelisted, skipping.`)
			return
		}

		if (currentStatus === 'PERM_BLACKLISTED') {
			status = 'PERM_BLACKLISTED'
		}

		const currentTypeIndex = typeHierarchy.indexOf(currentType)
		const newTypeIndex = typeHierarchy.indexOf(type)

		if (newTypeIndex > currentTypeIndex) {
			await connection.execute(
				'UPDATE Users SET type = ?, status = ? WHERE id = ?',
				[type, status, id]
			)
		} else {
			await connection.execute('UPDATE Users SET status = ? WHERE id = ?', [
				status,
				id,
			])
		}
	} else {
		await connection.execute(
			'INSERT INTO Users (id, last_username, avatar, type, status) VALUES (?, ?, ?, ?, ?)',
			[id, 'EMPTY', 'https://cdn.discordapp.com/embed/avatars/0.png', type, status]
		)
	}

	const rolesString = roles.length > 0 ? roles.join(', ') : ''

	const [importRows] = await connection.execute(
		'SELECT id, type FROM Imports WHERE id = ? AND server = ?',
		[id, serverId]
	)

	if (importRows.length > 0) {
		const currentType = importRows[0].type
		const currentTypeIndex = typeHierarchy.indexOf(currentType)
		const newTypeIndex = typeHierarchy.indexOf(type)

		if (newTypeIndex > currentTypeIndex) {
			await connection.execute(
				'UPDATE Imports SET type = ?, roles = ?, updatedAt = NOW(), appealed = 0 WHERE id = ? AND server = ?',
				[type, rolesString, id, serverId]
			)
			updated = updated + 1
		} else {
			await connection.execute(
				'UPDATE Imports SET roles = ?, appealed = 0, updatedAt = NOW() WHERE id = ? AND server = ?',
				[rolesString, id, serverId]
			)
			updated = updated + 1
		}
	} else {
		await connection.execute(
			'INSERT INTO Imports (id, server, roles, type, appealed, createdAt, updatedAt, reason) VALUES (?, ?, ?, ?, ?, NOW(), NOW(), ?)',
			[id, serverId, rolesString, type, 0, '']
		)
		added = added + 1
	}

	totalUsers['new'] += added
	totalUsers['updated'] += updated
}

const resetInactivityTimer = () => {
	if (inactivityTimer) {
		clearTimeout(inactivityTimer)
	}
	inactivityTimer = setTimeout(async () => {
		if (connection) {
			await connection.end()

			connection = false
			serverDoesntExist = false
			totalUsers = { new: 0, updated: 0 }

			console.log('Database connection closed due to inactivity')
		}
	}, 5 * 60 * 1000)
}

amqp.connect(connectionString, (error0, connection) => {
	if (error0) {
		throw error0
	}
	connection.createChannel(async (error1, channel) => {
		if (error1) {
			throw error1
		}

		var queue = 'users'

		channel.assertQueue(queue, {
			durable: false,
		})

		console.log('Waiting for guild and users')
		await sendDiscordWebhookMessage(`Ready to import users`)

		channel.consume(
			queue,
			async (msg) => {
				connection = await getConnection()
				resetInactivityTimer()

				const item = JSON.parse(msg.content)
				guildId = item.guildId
				if (item.id && !serverDoesntExist) await processUser(item.guildId, item)

				console.log(item)
			},
			{
				noAck: true,
			}
		)
	})
})
