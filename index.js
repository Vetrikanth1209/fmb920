const express = require('express');
const net = require('net');
const http = require('http');
const socketIo = require('socket.io');
const mongoose = require('mongoose');

// Initialize Express app and HTTP server
const app = express();
const server = http.createServer(app);
const io = socketIo(server);

// MongoDB setup (removed deprecated options)
mongoose.connect('mongodb+srv://vetrikanth:vetree1209@cluster0.vf6xd7d.mongodb.net/fmb920')
  .then(() => console.log('Connected to MongoDB'))
  .catch(err => console.error('MongoDB connection error:', err));

// MongoDB Schema
const deviceSchema = new mongoose.Schema({
  deviceId: String, // IMEI from FMB920
  timestamp: Number,
  gps: {
    latitude: Number,
    longitude: Number,
    altitude: Number,
    angle: Number,
    satellites: Number,
    speed: Number,
  },
  accelerometer: {
    x: Number,
    y: Number,
    z: Number,
  },
  priority: Number,
  createdAt: { type: Date, default: Date.now },
});

const Device = mongoose.model('Device', deviceSchema);

// TCP Server for FMB920 data
const tcpPort = 5000;
const tcpServer = net.createServer((socket) => {
  console.log('FMB920 connected:', socket.remoteAddress);

  socket.on('data', async (data) => {
    try {
      // Log raw data for debugging
      console.log('Raw data:', data.toString('hex'));

      // Parse Teltonika protocol data
      const records = parseTeltonikaData(data);
      if (!records) {
        console.error('Invalid data packet');
        return;
      }

      // Add deviceId (IMEI) - you may need to extract this from the packet header
      // For simplicity, assuming a single device; update as needed
      const deviceId = 'your-device-imei'; // Replace with actual IMEI extraction logic

      const formattedRecords = records.map(record => ({
        deviceId,
        timestamp: record.timestamp,
        gps: record.gps,
        accelerometer: record.accelerometer,
        priority: record.priority,
      }));

      // Save to MongoDB
      await Device.insertMany(formattedRecords);
      console.log('Data saved:', formattedRecords);

      // Broadcast to connected clients
      io.emit('deviceData', formattedRecords);

      // Send acknowledgment
      const numRecords = data.readUInt8(9); // Number of records at byte 9
      const ack = Buffer.alloc(4);
      ack.writeUInt32BE(numRecords, 0);
      socket.write(ack);
    } catch (err) {
      console.error('Error processing data:', err);
    }
  });

  socket.on('end', () => console.log('FMB920 disconnected'));
  socket.on('error', (err) => console.error('Socket error:', err));
});

// Start TCP server
tcpServer.listen(tcpPort, () => console.log(`TCP server listening on port ${tcpPort}`));

// Express routes
app.use(express.static('public')); // Serve static files from 'public' folder

app.get('/api/devices', async (req, res) => {
  try {
    const devices = await Device.find().sort({ timestamp: -1 }).limit(100);
    res.json(devices);
  } catch (err) {
    res.status(500).json({ error: 'Failed to fetch data' });
  }
});

// Parser for Teltonika Codec 8 Extended
function parseTeltonikaData(data) {
  try {
    // Verify Codec 8 Extended
    const codecId = data.readUInt8(8);
    if (codecId !== 0x8E) {
      console.error('Unsupported codec ID:', codecId);
      return null;
    }

    const numRecords = data.readUInt8(9);
    let offset = 10;
    const records = [];

    for (let i = 0; i < numRecords; i++) {
      const timestamp = Number(data.readBigInt64BE(offset));
      offset += 8;
      const priority = data.readUInt8(offset);
      offset += 1;

      // GPS data
      const longitude = data.readInt32BE(offset) / 10000000;
      offset += 4;
      const latitude = data.readInt32BE(offset) / 10000000;
      offset += 4;
      const altitude = data.readInt16BE(offset);
      offset += 2;
      const angle = data.readUInt16BE(offset);
      offset += 2;
      const satellites = data.readUInt8(offset);
      offset += 1;
      const speed = data.readUInt16BE(offset);
      offset += 2;

      // IO Elements
      const eventId = data.readUInt8(offset);
      offset += 1;
      const ioCount = data.readUInt8(offset);
      offset += 1;

      const ioData = {};
      for (let n = 1; n <= 4; n++) {
        const count = data.readUInt8(offset);
        offset += 1;
        for (let j = 0; j < count; j++) {
          const id = data.readUInt8(offset);
          offset += 1;
          let value;
          if (n === 1) value = data.readUInt8(offset);
          else if (n === 2) value = data.readUInt16BE(offset);
          else if (n === 4) value = data.readUInt32BE(offset);
          else value = Number(data.readBigInt64BE(offset));
          offset += n;
          ioData[id] = value;
        }
      }

      records.push({
        timestamp,
        priority,
        gps: { latitude, longitude, altitude, angle, satellites, speed },
        accelerometer: {
          x: ioData[251] || 0, // AVL ID 251 for X-axis (verify with manual)
          y: ioData[252] || 0, // AVL ID 252 for Y-axis
          z: ioData[253] || 0, // AVL ID 253 for Z-axis
        },
      });
    }
    return records;
  } catch (err) {
    console.error('Parsing error:', err);
    return null;
  }
}

// Start HTTP and Socket.IO server
const httpPort = 3000;
server.listen(httpPort, () => console.log(`HTTP server running on port ${httpPort}`));