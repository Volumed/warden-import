require('dotenv').config()
const axios = require('axios')
const { google } = require('googleapis')
const mysql = require('mysql2/promise')
const credentials =
	process.env.NODE_ENV === 'development'
		? require('./credentials-dev.json')
		: require('./credentials.json')
const path = require('path')
const webhookUrl = process.env.WEBHOOK_URL

let totalUsers = [];
totalUsers['new'] = 0;
totalUsers['updated'] = 0;
let unknowServers = ''
let startTime = performance.now()


async function authenticate() {
	const auth = new google.auth.GoogleAuth({
		credentials,
		scopes: ['https://www.googleapis.com/auth/drive'],
	})
	return auth.getClient()
}


function millisToMinutesAndSeconds(millis) {
	var minutes = Math.floor(millis / 60000);
	var seconds = ((millis % 60000) / 1000).toFixed(0);
	return minutes + ":" + (seconds < 10 ? '0' : '') + seconds;
  }
async function sendDiscordWebhookMessage(description, color = 0x00ff00) {
	try {
		const response = await axios.post(webhookUrl, {
			embeds: [
				{
					title: 'Import',
					description: description,
					color: color,
				},
			],
		})
		console.log('Message sent successfully:', response.data)
	} catch (error) {
		console.error('Error sending message:', error)
	}
}

async function listAndReadJsonFiles(auth) {
	const drive = google.drive({ version: 'v3', auth })
	let pageToken = null
	do {
		try {
			const res = await drive.files.list({
				q: "mimeType='application/json'",
				pageSize: 100,
				fields: 'nextPageToken, files(id, name)',
				pageToken: pageToken,
			})
			const files = res.data.files
			if (files.length) {
				console.log('Files:')
				for (const file of files) {
					console.log(`${file.name} (${file.id})`)
					await readFileContent(drive, file.id, file.name)
					await deleteFile(drive, file.id)
				}
				let endTime = performance.now()
				await sendDiscordWebhookMessage(
					`All **${files.length}** file(s) have been processed. Total New users:**${totalUsers['new']}** / Total updated users:**${totalUsers['updated']}**  / Total Time taken: **${millisToMinutesAndSeconds(endTime - startTime)}** / Servers not found: ${unknowServers}`
				)
			} else {
				console.log('No files found.')
				await sendDiscordWebhookMessage(`No files found to import.`, 0xff0000)
			}
			pageToken = res.data.nextPageToken
		} catch (error) {
			console.error('Error listing files:', error)
			break
		}
	} while (pageToken)
}

async function readFileContent(drive, fileId, fileName) {
	try {
		const res = await drive.files.get(
			{ fileId, alt: 'media' },
			{ responseType: 'stream' }
		)
		let data = ''
		res.data.on('data', (chunk) => {
			data += chunk
		})
		res.data.on('end', async () => {
			const jsonData = JSON.parse(data)
			await processJsonData(jsonData, fileName)
		})
	} catch (error) {
		console.error(`Error reading file ${fileName}:`, error)
	}
}

async function processJsonData(jsonData, fileName) {
	const connection = await mysql.createConnection({
		host: process.env.DB_HOST,
		user: process.env.DB_USER,
		password: process.env.DB_PASSWORD,
		database: process.env.DB_NAME,
	})

	const typeHierarchy = ['OTHER', 'LEAKER', 'CHEATER', 'SUPPORTER', 'OWNER']

	const serverId = path.basename(fileName, '.json').split('-')[1]

	const [serverRows] = await connection.execute(
		'SELECT id FROM BadServers WHERE id = ?',
		[serverId]
	)

	if (serverRows.length === 0) {
		console.error(`Server ID ${serverId} does not exist in the BadServers table.`)
		unknowServers += ` / ${serverId}`;
		await sendDiscordWebhookMessage(
			`Server ID **${serverId}** wasn't found in the bad servers list.`,
			0xff0000
		)
		await connection.end()
		return
	}

	await sendDiscordWebhookMessage(
		`Importing **${jsonData.length}** users for **${serverId}**...`
	)
	let added = 0
	let updated = 0

	for (const entry of jsonData) {
		const { id, type, roles = [] } = entry

		if (!id || !type) {
			continue
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
				continue
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
	}
	totalUsers['new'] += added;
	totalUsers['updated'] += updated;
	await sendDiscordWebhookMessage(
		`Imported **${added}** new users and updated **${updated}** users for **${serverId}**.`
	)
	await connection.end()
}

async function deleteFile(drive, fileId) {
	try {
		await drive.files.delete({ fileId })
		console.log(`File ${fileId} deleted successfully.`)
	} catch (error) {
		console.error(`Error deleting file ${fileId}:`, error)
	}
}

authenticate()
	.then((auth) => listAndReadJsonFiles(auth))
	.catch(console.error)
