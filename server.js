require('dotenv').config();
const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');
const mongoose = require('mongoose');
const { Redis } = require('@upstash/redis');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const cookieParser = require('cookie-parser');

const app = express();
app.use(express.json());
app.use(cookieParser());

// Serve static frontend files from the "public" directory
app.use(express.static(path.join(__dirname, 'public')));

const JWT_SECRET = process.env.JWT_SECRET || 'local_chat_app_development_secret_key_987654321';

// 1. DATABASE & CACHE INTEGRATION WITH MEMORY FALLBACKS
let isMongoConnected = false;
let isRedisConnected = false;

const mongoUri = process.env.MONGODB_URI;

const localDbMock = {
    users: [],
    messages: [],
    groups: [],
    async findUser(username) {
        return this.users.find(u => u.username.toLowerCase() === username.toLowerCase());
    },
    async getAllUsers() {
        return this.users.map(u => ({ username: u.username, avatar: u.avatar }));
    },
    async createUser(username, hashedPassword, avatar) {
        const newUser = {
            _id: 'mock_u_' + Math.random().toString(36).substr(2, 9),
            username,
            password: hashedPassword,
            avatar: avatar || `https://api.dicebear.com/7.x/bottts/svg?seed=${username}`,
            createdAt: new Date()
        };
        this.users.push(newUser);
        return newUser;
    },
    async getRecentMessages(roomId = 'lounge', limit = 50) {
        return this.messages.filter(m => m.roomId === roomId).slice(-limit);
    },
    async saveMessage(sender, content, type = 'chat', roomId = 'lounge') {
        const newMsg = {
            _id: 'mock_m_' + Math.random().toString(36).substr(2, 9),
            sender,
            content,
            type,
            roomId,
            timestamp: new Date()
        };
        this.messages.push(newMsg);
        return newMsg;
    },
    async createGroup(name, creator, members = [], avatar) {
        const newGroup = {
            _id: 'mock_g_' + Math.random().toString(36).substr(2, 9),
            name,
            creator,
            members: Array.from(new Set([creator, ...members])),
            avatar: avatar || `https://api.dicebear.com/7.x/initials/svg?seed=${encodeURIComponent(name)}&backgroundColor=6a11cb,2575fc&color=ffffff`,
            createdAt: new Date()
        };
        this.groups.push(newGroup);
        return newGroup;
    },
    async getGroupsForUser(username) {
        return this.groups.filter(g => g.members.includes(username));
    },
    async leaveGroup(groupId, username) {
        const group = this.groups.find(g => g._id === groupId);
        if (group) {
            group.members = group.members.filter(m => m !== username);
        }
        return group;
    },
    async updateGroupMembers(groupId, members) {
        const group = this.groups.find(g => g._id === groupId);
        if (group) {
            group.members = Array.from(new Set([group.creator, ...members]));
        }
        return group;
    }
};

let User, Message, Group;

if (mongoUri) {
    console.log('[SYSTEM] Attempting to connect to MongoDB Atlas...');
    mongoose.connect(mongoUri)
        .then(() => {
            isMongoConnected = true;
            console.log('[DATABASE] Successfully connected to MongoDB Atlas!');
        })
        .catch(err => {
            console.warn('[DATABASE] MongoDB connection failed! Falling back to IN-MEMORY DATABASE.', err.message);
        });

    const userSchema = new mongoose.Schema({
        username: { type: String, required: true, unique: true, lowercase: true, trim: true },
        password: { type: String, required: true },
        avatar: { type: String },
        createdAt: { type: Date, default: Date.now }
    });
    User = mongoose.model('User', userSchema);

    const messageSchema = new mongoose.Schema({
        sender: { type: String, required: true },
        content: { type: String, required: true },
        type: { type: String, default: 'chat' },
        roomId: { type: String, default: 'lounge', index: true },
        timestamp: { type: Date, default: Date.now }
    });
    Message = mongoose.model('Message', messageSchema);

    const groupSchema = new mongoose.Schema({
        name: { type: String, required: true },
        creator: { type: String, required: true },
        members: [{ type: String }],
        avatar: { type: String },
        createdAt: { type: Date, default: Date.now }
    });
    Group = mongoose.model('Group', groupSchema);
} else {
    console.warn('[SYSTEM] MONGODB_URI is not defined. Falling back to IN-MEMORY DATABASE.');
}

const redisUrl = process.env.UPSTASH_REDIS_REST_URL;
const redisToken = process.env.UPSTASH_REDIS_REST_TOKEN;
let redis;

const localRedisMock = {
    onlineUsers: new Set(),
    cachedMessages: {},
    async sadd(key, member) { this.onlineUsers.add(member); return 1; },
    async srem(key, member) { return this.onlineUsers.delete(member) ? 1 : 0; },
    async smembers(key) { return Array.from(this.onlineUsers); },
    async lrange(key, start, stop) {
        if (!this.cachedMessages[key]) return [];
        return this.cachedMessages[key].slice(start, stop === -1 ? undefined : stop + 1);
    },
    async rpush(key, value) {
        if (!this.cachedMessages[key]) this.cachedMessages[key] = [];
        const parsed = typeof value === 'string' ? JSON.parse(value) : value;
        this.cachedMessages[key].push(parsed);
        return this.cachedMessages[key].length;
    },
    async ltrim(key, start, stop) {
        if (!this.cachedMessages[key]) return 'OK';
        this.cachedMessages[key] = this.cachedMessages[key].slice(start, stop === -1 ? undefined : stop + 1);
        return 'OK';
    }
};

if (redisUrl && redisToken) {
    try {
        redis = new Redis({ url: redisUrl, token: redisToken });
        isRedisConnected = true;
        console.log('[CACHE] Successfully connected to Upstash Redis REST Client!');
    } catch (err) {
        console.warn('[CACHE] Upstash Redis configuration failed! Falling back to IN-MEMORY REDIS MOCK.', err.message);
    }
} else {
    console.warn('[SYSTEM] Redis variables not defined. Falling back to IN-MEMORY REDIS MOCK.');
}

const getRedisClient = () => isRedisConnected ? redis : localRedisMock;

// 2. AUTHENTICATION MIDDLEWARE & ENDPOINTS
const authenticateToken = async (req, res, next) => {
    let token = req.cookies.token;
    if (!token && req.headers['authorization']) {
        const authHeader = req.headers['authorization'];
        if (authHeader.startsWith('Bearer ')) {
            token = authHeader.split(' ')[1];
        }
    }
    if (!token) {
        return res.status(401).json({ error: 'Access denied. No token provided.' });
    }
    try {
        const decoded = jwt.verify(token, JWT_SECRET);
        req.user = decoded;
        next();
    } catch (err) {
        res.status(403).json({ error: 'Invalid or expired authentication token.' });
    }
};

app.post('/api/auth/signup', async (req, res) => {
    try {
        let { username, password, avatar } = req.body;
        if (!username || !password) {
            return res.status(400).json({ error: 'Username and password are required.' });
        }
        username = username.trim();
        if (username.length < 3 || username.length > 15) {
            return res.status(400).json({ error: 'Username must be between 3 and 15 characters.' });
        }

        let userExists = false;
        if (isMongoConnected) {
            const existingUser = await User.findOne({ username: username.toLowerCase() });
            if (existingUser) userExists = true;
        } else {
            const existingUser = await localDbMock.findUser(username);
            if (existingUser) userExists = true;
        }

        if (userExists) {
            return res.status(400).json({ error: 'Username is already taken.' });
        }

        const salt = await bcrypt.genSalt(10);
        const hashedPassword = await bcrypt.hash(password, salt);

        if (!avatar) {
            avatar = `https://api.dicebear.com/7.x/initials/svg?seed=${encodeURIComponent(username)}&backgroundColor=00f2fe,4facfe&color=ffffff`;
        }

        let user;
        if (isMongoConnected) {
            user = new User({ username, password: hashedPassword, avatar });
            await user.save();
        } else {
            user = await localDbMock.createUser(username, hashedPassword, avatar);
        }

        const token = jwt.sign({ id: user._id, username: user.username, avatar: user.avatar }, JWT_SECRET, { expiresIn: '7d' });
        res.cookie('token', token, { httpOnly: true, secure: process.env.NODE_ENV === 'production', maxAge: 7 * 24 * 60 * 60 * 1000 });

        try {
            const payload = JSON.stringify({ type: 'user_registered', user: { username: user.username, avatar: user.avatar } });
            wss.clients.forEach(c => {
                if (c.readyState === WebSocket.OPEN) c.send(payload);
            });
        } catch (wsErr) {
            console.warn('[WS BROADCAST ERROR]', wsErr.message);
        }

        res.status(201).json({ success: true, token, user: { id: user._id, username: user.username, avatar: user.avatar } });
    } catch (error) {
        console.error('[SIGNUP ERROR]', error);
        res.status(500).json({ error: 'An internal server error occurred.' });
    }
});

app.post('/api/auth/login', async (req, res) => {
    try {
        let { username, password } = req.body;
        if (!username || !password) return res.status(400).json({ error: 'Username and password are required.' });
        username = username.trim();

        let user = isMongoConnected ? await User.findOne({ username: username.toLowerCase() }) : await localDbMock.findUser(username);
        if (!user || !(await bcrypt.compare(password, user.password))) {
            return res.status(400).json({ error: 'Invalid username or password.' });
        }

        const token = jwt.sign({ id: user._id, username: user.username, avatar: user.avatar }, JWT_SECRET, { expiresIn: '7d' });
        res.cookie('token', token, { httpOnly: true, secure: process.env.NODE_ENV === 'production', maxAge: 7 * 24 * 60 * 60 * 1000 });

        res.status(200).json({ success: true, token, user: { id: user._id, username: user.username, avatar: user.avatar } });
    } catch (error) {
        console.error('[LOGIN ERROR]', error);
        res.status(500).json({ error: 'Internal server error during login.' });
    }
});

app.get('/api/auth/me', authenticateToken, (req, res) => {
    res.status(200).json({ success: true, user: req.user });
});

app.post('/api/auth/logout', (req, res) => {
    res.clearCookie('token');
    res.status(200).json({ success: true, message: 'Logged out successfully.' });
});

app.get('/api/users', authenticateToken, async (req, res) => {
    try {
        let usersList = isMongoConnected ? 
            (await User.find({}, 'username avatar')).map(u => ({ username: u.username, avatar: u.avatar })) : 
            await localDbMock.getAllUsers();
        res.status(200).json({ success: true, users: usersList });
    } catch (error) {
        res.status(500).json({ error: 'Failed to retrieve users.' });
    }
});

// 3. GROUPS ROUTING LAYERS
app.get('/api/groups', authenticateToken, async (req, res) => {
    try {
        let groupsList = isMongoConnected ? await Group.find({ members: req.user.username }) : await localDbMock.getGroupsForUser(req.user.username);
        res.status(200).json({ success: true, groups: groupsList });
    } catch (error) {
        res.status(500).json({ error: 'Failed to retrieve groups.' });
    }
});

app.post('/api/groups', authenticateToken, async (req, res) => {
    try {
        const { name, members = [] } = req.body;
        if (!name || !name.trim()) return res.status(400).json({ error: 'Group name is required.' });
        if (!members || members.length === 0) return res.status(400).json({ error: 'At least one member must be selected.' });

        const creator = req.user.username;
        let finalMembers = Array.from(new Set([creator, ...members]));
        const groupAvatar = `https://api.dicebear.com/7.x/initials/svg?seed=${encodeURIComponent(name.trim())}&backgroundColor=6a11cb,2575fc&color=ffffff`;

        let newGroup;
        if (isMongoConnected) {
            newGroup = new Group({ name: name.trim(), creator, members: finalMembers, avatar: groupAvatar });
            await newGroup.save();
        } else {
            newGroup = await localDbMock.createGroup(name.trim(), creator, finalMembers, groupAvatar);
        }

        const groupCreatedPayload = JSON.stringify({ type: 'group_created', group: newGroup });
        finalMembers.forEach(memberUsername => {
            const socketSet = userSockets.get(memberUsername);
            if (socketSet) {
                socketSet.forEach(s => {
                    if (s.readyState === WebSocket.OPEN) s.send(groupCreatedPayload);
                });
            }
        });

        res.status(201).json({ success: true, group: newGroup });
    } catch (error) {
        res.status(500).json({ error: 'Failed to create group.' });
    }
});

app.post('/api/groups/:id/leave', authenticateToken, async (req, res) => {
    try {
        const groupId = req.params.id;
        const username = req.user.username;
        let group = isMongoConnected ? await Group.findById(groupId) : localDbMock.groups.find(g => g._id === groupId);

        if (!group) return res.status(404).json({ error: 'Group not found.' });
        if (group.creator === username) return res.status(400).json({ error: 'Group creator cannot leave.' });

        if (isMongoConnected) {
            group.members = group.members.filter(m => m !== username);
            await group.save();
        } else {
            group = await localDbMock.leaveGroup(groupId, username);
        }

        const broadcastPayload = JSON.stringify({
            type: 'chat',
            roomId: `group_${group._id}`,
            username: 'System',
            avatar: 'https://api.dicebear.com/7.x/bottts/svg?seed=system',
            message: `@${username} left the group.`,
            timestamp: new Date()
        });

        group.members.forEach(u => {
            const socketSet = userSockets.get(u);
            if (socketSet) {
                socketSet.forEach(s => {
                    if (s.readyState === WebSocket.OPEN) s.send(broadcastPayload);
                });
            }
        });

        res.status(200).json({ success: true, group });
    } catch (err) {
        res.status(500).json({ error: 'Failed to leave group.' });
    }
});

app.put('/api/groups/:id/members', authenticateToken, async (req, res) => {
    try {
        const groupId = req.params.id;
        const { members } = req.body;
        const username = req.user.username;
        if (!members || members.length === 0) return res.status(400).json({ error: 'Group must have at least one member.' });

        let group = isMongoConnected ? await Group.findById(groupId) : localDbMock.groups.find(g => g._id === groupId);
        if (!group) return res.status(404).json({ error: 'Group not found.' });
        if (group.creator !== username) return res.status(403).json({ error: 'Only creators manage members.' });

        if (isMongoConnected) {
            group.members = Array.from(new Set([group.creator, ...members]));
            await group.save();
        } else {
            group = await localDbMock.updateGroupMembers(groupId, members);
        }

        const broadcastPayload = JSON.stringify({
            type: 'chat',
            roomId: `group_${group._id}`,
            username: 'System',
            avatar: 'https://api.dicebear.com/7.x/bottts/svg?seed=system',
            message: `@${username} updated the group members.`,
            timestamp: new Date()
        });

        const updatePayload = JSON.stringify({ type: 'group_updated', group });

        group.members.forEach(u => {
            const socketSet = userSockets.get(u);
            if (socketSet) {
                socketSet.forEach(s => {
                    if (s.readyState === WebSocket.OPEN) {
                        s.send(broadcastPayload);
                        s.send(updatePayload);
                    }
                });
            }
        });

        res.status(200).json({ success: true, group });
    } catch (err) {
        res.status(500).json({ error: 'Failed to update members.' });
    }
});

// 4. WEBSOCKET SERVER & EVENT ROUTING PIPELINE
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });
const userSockets = new Map();

async function broadcastOnlineUsers() {
    try {
        const client = getRedisClient();
        const onlineUsers = await client.smembers('online_users');
        const onlineUserList = onlineUsers.map(username => {
            const socketSet = userSockets.get(username);
            let avatar = (socketSet && socketSet.size > 0) ? Array.from(socketSet)[0].avatar : '';
            if (!avatar) {
                avatar = `https://api.dicebear.com/7.x/initials/svg?seed=${encodeURIComponent(username)}&backgroundColor=00f2fe,4facfe&color=ffffff`;
            }
            return { username, avatar };
        });

        const payload = JSON.stringify({ type: 'online_list', users: onlineUserList });
        wss.clients.forEach(client => {
            if (client.readyState === WebSocket.OPEN) client.send(payload);
        });
    } catch (err) {
        console.error('[PRESENCE BROADCAST ERROR]', err);
    }
}

wss.on('connection', async (ws, req) => {
    const urlParams = new URLSearchParams(req.url.split('?')[1]);
    const token = urlParams.get('token');
    let decodedUser;

    try {
        if (!token) throw new Error('No token provided.');
        decodedUser = jwt.verify(token, JWT_SECRET);
    } catch (err) {
        ws.send(JSON.stringify({ type: 'system', message: 'Authentication failed. Closing connection.' }));
        ws.close(4001, 'Unauthorized');
        return;
    }

    const { username, avatar } = decodedUser;
    ws.username = username;
    ws.avatar = avatar;

    if (!userSockets.has(username)) userSockets.set(username, new Set());
    userSockets.get(username).add(ws);

    const client = getRedisClient();
    await client.sadd('online_users', username);

    ws.send(JSON.stringify({ type: 'system', message: `Welcome to the secure chat, @${username}! Connection authenticated.` }));
    await broadcastOnlineUsers();

    // Fetch lounge history on connect
    try {
        const cacheKey = 'chat_history:lounge';
        const cachedRaw = await client.lrange(cacheKey, 0, -1);
        let history = [];

        if (cachedRaw && cachedRaw.length > 0) {
            history = cachedRaw.map(item => typeof item === 'string' ? JSON.parse(item) : item);
        } else {
            if (isMongoConnected) {
                const dbMsgs = await Message.find({ roomId: 'lounge' }).sort({ timestamp: -1 }).limit(50);
                history = dbMsgs.reverse().map(m => ({
                    type: 'chat',
                    roomId: 'lounge',
                    username: m.sender,
                    message: m.content,
                    timestamp: m.timestamp,
                    avatar: `https://api.dicebear.com/7.x/initials/svg?seed=${encodeURIComponent(m.sender)}&backgroundColor=00f2fe,4facfe&color=ffffff`
                }));
            } else {
                history = await localDbMock.getRecentMessages('lounge', 50);
            }
            for (const msg of history) {
                await client.rpush(cacheKey, msg);
            }
        }
        await client.ltrim(cacheKey, -50, -1);
        if (history.length > 0) ws.send(JSON.stringify({ type: 'history', roomId: 'lounge', messages: history }));
    } catch (err) {
        console.error('[WS HISTORY FETCH ERROR]', err.message);
    }

    ws.on('message', async (rawData) => {
        try {
            const parsedData = JSON.parse(rawData.toString());

            const checkGroupMembership = async (roomId, uname) => {
                if (!roomId.startsWith('group_')) return true;
                const groupId = roomId.replace('group_', '');
                if (isMongoConnected) {
                    if (!mongoose.Types.ObjectId.isValid(groupId)) return false;
                    const grp = await Group.findById(groupId);
                    return grp && grp.members.includes(uname);
                } else {
                    const grp = localDbMock.groups.find(g => g._id === groupId);
                    return grp && grp.members.includes(uname);
                }
            };

            if (parsedData.type === 'get_history') {
                const targetRoomId = parsedData.roomId || 'lounge';
                if (!(await checkGroupMembership(targetRoomId, username))) {
                    ws.send(JSON.stringify({ type: 'system', message: 'Access denied: Membership missing.' }));
                    return;
                }
                const cacheKey = `chat_history:${targetRoomId}`;
                const cachedRaw = await client.lrange(cacheKey, 0, -1);
                let history = [];

                if (cachedRaw && cachedRaw.length > 0) {
                    history = cachedRaw.map(item => typeof item === 'string' ? JSON.parse(item) : item);
                } else {
                    if (isMongoConnected) {
                        const dbMsgs = await Message.find({ roomId: targetRoomId }).sort({ timestamp: -1 }).limit(50);
                        history = dbMsgs.reverse().map(m => ({
                            type: 'chat',
                            roomId: targetRoomId,
                            username: m.sender,
                            message: m.content,
                            timestamp: m.timestamp,
                            avatar: `https://api.dicebear.com/7.x/initials/svg?seed=${encodeURIComponent(m.sender)}&backgroundColor=00f2fe,4facfe&color=ffffff`
                        }));
                    } else {
                        history = await localDbMock.getRecentMessages(targetRoomId, 50);
                    }
                    for (const msg of history) {
                        await client.rpush(cacheKey, msg);
                    }
                }
                await client.ltrim(cacheKey, -50, -1);
                ws.send(JSON.stringify({ type: 'history', roomId: targetRoomId, messages: history }));
                return;
            }

            const content = parsedData.message;
            if (!content || content.trim() === '') return;
            const targetRoomId = parsedData.roomId || 'lounge';

            if (!(await checkGroupMembership(targetRoomId, username))) {
                ws.send(JSON.stringify({ type: 'system', message: 'Access denied.' }));
                return;
            }

            const payload = {
                type: 'chat',
                roomId: targetRoomId,
                username,
                avatar,
                message: content.trim(),
                timestamp: new Date()
            };

            if (isMongoConnected) {
                const newDbMsg = new Message({ sender: username, content: content.trim(), roomId: targetRoomId });
                await newDbMsg.save();
            } else {
                await localDbMock.saveMessage(username, content.trim(), 'chat', targetRoomId);
            }

            const cacheKey = `chat_history:${targetRoomId}`;
            await client.rpush(cacheKey, payload);
            await client.ltrim(cacheKey, -50, -1);

            const broadcastPayload = JSON.stringify(payload);

            if (targetRoomId === 'lounge') {
                wss.clients.forEach(c => {
                    if (c.readyState === WebSocket.OPEN) c.send(broadcastPayload);
                });
            } else if (targetRoomId.startsWith('dm_')) {
                let otherUser = targetRoomId.substring(3);
                if (otherUser.startsWith(`${username}_`)) {
                    otherUser = otherUser.substring(username.length + 1);
                } else if (otherUser.endsWith(`_${username}`)) {
                    otherUser = otherUser.substring(0, otherUser.length - username.length - 1);
                }
                [username, otherUser].forEach(u => {
                    const socketSet = userSockets.get(u);
                    if (socketSet) {
                        socketSet.forEach(s => {
                            if (s.readyState === WebSocket.OPEN) s.send(broadcastPayload);
                        });
                    }
                });
            } else if (targetRoomId.startsWith('group_')) {
                const groupId = targetRoomId.replace('group_', '');
                let groupMembers = [];
                if (isMongoConnected) {
                    const grp = await Group.findById(groupId);
                    if (grp) groupMembers = grp.members;
                } else {
                    const grp = localDbMock.groups.find(g => g._id === groupId);
                    if (grp) groupMembers = grp.members;
                }
                groupMembers.forEach(u => {
                    const socketSet = userSockets.get(u);
                    if (socketSet) {
                        socketSet.forEach(s => {
                            if (s.readyState === WebSocket.OPEN) s.send(broadcastPayload);
                        });
                    }
                });
            }
        } catch (error) {
            console.error('[WS ERROR]', error.message);
        }
    });

    ws.on('close', async () => {
        const socketSet = userSockets.get(username);
        if (socketSet) {
            socketSet.delete(ws);
            if (socketSet.size === 0) {
                userSockets.delete(username);
                await client.srem('online_users', username);
                await broadcastOnlineUsers();
            }
        }
    });
});

app.post('/api/notify', authenticateToken, async (req, res) => {
    const { notification } = req.body;
    if (!notification) return res.status(400).json({ error: "Missing parameter." });

    const notificationPayload = JSON.stringify({ type: 'notification', message: notification, timestamp: new Date() });
    let activeReceivers = 0;
    wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(notificationPayload);
            activeReceivers++;
        }
    });
    res.status(200).json({ success: true, message: `Notification pushed to ${activeReceivers} client sessions.` });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`[SYSTEM] Server listening on http://localhost:${PORT}`);
    console.log(`[SYSTEM] Sandboxed Mocks activated for zero-config workspace!`);
});