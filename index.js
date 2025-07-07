const express = require('express');
const net = require('net');
const mongoose = require('mongoose');
const cors = require('cors');
require('dotenv').config();

const app = express();

app.set('trust proxy', true);

// Enhanced logging utility
const logger = {
  info: (message, data = null) => {
    const timestamp = new Date().toISOString();
    console.log(`[${timestamp}] INFO: ${message}`, data ? JSON.stringify(data, null, 2) : '');
  },
  error: (message, error = null) => {
    const timestamp = new Date().toISOString();
    console.error(`[${timestamp}] ERROR: ${message}`);
    if (error) {
      console.error('Error details:', error);
      if (error.stack) console.error('Stack trace:', error.stack);
    }
  },
  warn: (message, data = null) => {
    const timestamp = new Date().toISOString();
    console.warn(`[${timestamp}] WARN: ${message}`, data ? JSON.stringify(data, null, 2) : '');
  },
  debug: (message, data = null) => {
    if (process.env.DEBUG === 'true') {
      const timestamp = new Date().toISOString();
      console.log(`[${timestamp}] DEBUG: ${message}`, data ? JSON.stringify(data, null, 2) : '');
    }
  }
};

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Request logging middleware
app.use((req, res, next) => {
  logger.info(`${req.method} ${req.path}`, {
    ip: req.ip,
    userAgent: req.get('User-Agent'),
    query: req.query,
    body: req.method === 'POST' ? req.body : undefined
  });
  next();
});

logger.info('Server initializing...', {
  nodeVersion: process.version,
  environment: process.env.NODE_ENV || 'development'
});

// MongoDB Schema for tracker data
const trackerDataSchema = new mongoose.Schema({
  deviceId: String,
  timestamp: Date,
  latitude: Number,
  longitude: Number,
  altitude: Number,
  speed: Number,
  direction: Number,
  satellites: Number,
  hdop: Number,
  battery: Number,
  ignition: Boolean,
  movement: Boolean,
  gsm: Number,
  temperature: Number,
  digitalInputs: [Number],
  analogInputs: [Number],
  rawData: String,
  createdAt: { type: Date, default: Date.now }
});

const TrackerData = mongoose.model('TrackerData', trackerDataSchema);

// MongoDB connection
logger.info('Connecting to MongoDB...', { uri: process.env.MONGODB_URI || 'mongodb://localhost:27017/tracker_db' });

mongoose.connect(process.env.MONGODB_URI || 'mongodb://localhost:27017/tracker_db')
  .then(() => {
    logger.info('Successfully connected to MongoDB', {
      host: mongoose.connection.host,
      name: mongoose.connection.name,
      readyState: mongoose.connection.readyState
    });
  })
  .catch(err => {
    logger.error('MongoDB connection failed', err);
    process.exit(1);
  });

// MongoDB connection event handlers
mongoose.connection.on('error', (err) => {
  logger.error('MongoDB connection error', err);
});

mongoose.connection.on('disconnected', () => {
  logger.warn('MongoDB disconnected');
});

mongoose.connection.on('reconnected', () => {
  logger.info('MongoDB reconnected');
});

// Teltonika Protocol Parser
class TeltonikaParser {
  static parseCodec8(buffer) {
    try {
      logger.debug('Starting Codec8 parsing', { 
        bufferLength: buffer.length, 
        bufferHex: buffer.toString('hex').substring(0, 50) + '...' 
      });
      
      let offset = 0;
      
      // Skip preamble (4 bytes)
      const preamble = buffer.readUInt32BE(offset);
      logger.debug('Preamble read', { preamble: preamble.toString(16) });
      offset += 4;
      
      // Data field length (4 bytes)
      const dataLength = buffer.readUInt32BE(offset);
      logger.debug('Data length', { dataLength });
      offset += 4;
      
      // Codec ID (1 byte) - should be 0x08 for Codec 8
      const codecId = buffer.readUInt8(offset);
      logger.debug('Codec ID', { codecId: codecId.toString(16) });
      offset += 1;
      
      if (codecId !== 0x08) {
        logger.error('Unsupported codec detected', { codecId: codecId.toString(16) });
        throw new Error('Unsupported codec: ' + codecId);
      }
      
      // Number of data records (1 byte)
      const recordCount = buffer.readUInt8(offset);
      logger.debug('Record count', { recordCount });
      offset += 1;
      
      const records = [];
      
      for (let i = 0; i < recordCount; i++) {
        logger.debug(`Parsing record ${i + 1}/${recordCount}`);
        const record = this.parseAvlRecord(buffer, offset);
        records.push(record.data);
        offset = record.offset;
        logger.debug(`Record ${i + 1} parsed successfully`, { 
          lat: record.data.latitude, 
          lng: record.data.longitude,
          timestamp: record.data.timestamp
        });
      }
      
      logger.info('Codec8 parsing completed successfully', { recordCount: records.length });
      return records;
    } catch (error) {
      logger.error('Codec8 parsing failed', error);
      return null;
    }
  }
  
  static parseAvlRecord(buffer, offset) {
    try {
      logger.debug('Parsing AVL record', { startOffset: offset });
      
      // Timestamp (8 bytes) - milliseconds since 1970-01-01 00:00:00 UTC
      const timestamp = new Date(Number(buffer.readBigUInt64BE(offset)));
      logger.debug('Timestamp parsed', { timestamp });
      offset += 8;
      
      // Priority (1 byte)
      const priority = buffer.readUInt8(offset);
      logger.debug('Priority', { priority });
      offset += 1;
      
      // GPS Element
      const longitude = buffer.readInt32BE(offset) / 10000000;
      offset += 4;
      
      const latitude = buffer.readInt32BE(offset) / 10000000;
      offset += 4;
      
      logger.debug('GPS coordinates', { latitude, longitude });
      
      const altitude = buffer.readUInt16BE(offset);
      offset += 2;
      
      const direction = buffer.readUInt16BE(offset);
      offset += 2;
      
      const satellites = buffer.readUInt8(offset);
      offset += 1;
      
      const speed = buffer.readUInt16BE(offset);
      offset += 2;
      
      logger.debug('GPS details', { altitude, direction, satellites, speed });
      
      // IO Element
      const eventId = buffer.readUInt8(offset);
      offset += 1;
      
      const totalElements = buffer.readUInt8(offset);
      offset += 1;
      
      logger.debug('IO elements', { eventId, totalElements });
      
      // Parse IO elements
      const ioData = this.parseIOElements(buffer, offset);
      offset = ioData.offset;
      
      const recordData = {
        timestamp,
        priority,
        longitude,
        latitude,
        altitude,
        direction,
        satellites,
        speed,
        eventId,
        ...ioData.elements
      };
      
      logger.debug('AVL record parsed successfully', { recordData });
      
      return {
        data: recordData,
        offset
      };
    } catch (error) {
      logger.error('Error parsing AVL record', error);
      throw error;
    }
  }
  
  static parseIOElements(buffer, offset) {
    try {
      logger.debug('Parsing IO elements', { startOffset: offset });
      
      const elements = {
        digitalInputs: [],
        analogInputs: [],
        ignition: false,
        movement: false,
        gsm: 0,
        battery: 0,
        temperature: 0
      };
      
      // 1 byte IO elements
      const oneByteCount = buffer.readUInt8(offset);
      logger.debug('1-byte IO elements count', { oneByteCount });
      offset += 1;
      
      for (let i = 0; i < oneByteCount; i++) {
        const id = buffer.readUInt8(offset);
        offset += 1;
        const value = buffer.readUInt8(offset);
        offset += 1;
        
        logger.debug(`1-byte IO element`, { id, value });
        
        // Map common IO IDs
        switch (id) {
          case 1: elements.digitalInputs.push(value); break;
          case 21: elements.gsm = value; break;
          case 66: elements.battery = value; break;
          case 239: elements.ignition = value === 1; break;
          case 240: elements.movement = value === 1; break;
          default: logger.debug('Unknown 1-byte IO ID', { id, value });
        }
      }
      
      // 2 byte IO elements
      const twoByteCount = buffer.readUInt8(offset);
      logger.debug('2-byte IO elements count', { twoByteCount });
      offset += 1;
      
      for (let i = 0; i < twoByteCount; i++) {
        const id = buffer.readUInt8(offset);
        offset += 1;
        const value = buffer.readUInt16BE(offset);
        offset += 2;
        
        logger.debug(`2-byte IO element`, { id, value });
        
        if (id === 72) elements.temperature = value / 10; // Temperature sensor
      }
      
      // 4 byte IO elements
      const fourByteCount = buffer.readUInt8(offset);
      logger.debug('4-byte IO elements count', { fourByteCount });
      offset += 1;
      
      for (let i = 0; i < fourByteCount; i++) {
        const id = buffer.readUInt8(offset);
        offset += 1;
        const value = buffer.readUInt32BE(offset);
        offset += 4;
        
        logger.debug(`4-byte IO element`, { id, value });
        elements.analogInputs.push(value);
      }
      
      // 8 byte IO elements
      const eightByteCount = buffer.readUInt8(offset);
      logger.debug('8-byte IO elements count', { eightByteCount });
      offset += 1;
      
      for (let i = 0; i < eightByteCount; i++) {
        const id = buffer.readUInt8(offset);
        offset += 1;
        const value = buffer.readBigUInt64BE(offset);
        offset += 8;
        
        logger.debug(`8-byte IO element`, { id, value: value.toString() });
      }
      
      logger.debug('IO elements parsed successfully', { elements });
      return { elements, offset };
    } catch (error) {
      logger.error('Error parsing IO elements', error);
      throw error;
    }
  }
}

// Store connected devices
const connectedDevices = new Map();

// TCP Server for Tracker Connection
const trackerServer = net.createServer((socket) => {
  const clientInfo = `${socket.remoteAddress}:${socket.remotePort}`;
  logger.info('New tracker connection established', { 
    clientAddress: socket.remoteAddress,
    clientPort: socket.remotePort,
    localAddress: socket.localAddress,
    localPort: socket.localPort
  });
  
  let deviceId = null;
  let buffer = Buffer.alloc(0);
  let connectionStartTime = new Date();
  
  // Set socket timeout
  socket.setTimeout(300000); // 5 minutes timeout
  
  socket.on('timeout', () => {
    logger.warn('Socket timeout occurred', { deviceId, clientInfo });
    socket.destroy();
  });
  
  socket.on('data', async (data) => {
    try {
      logger.debug('Raw data received', { 
        deviceId, 
        dataLength: data.length, 
        dataHex: data.toString('hex').substring(0, 100) + (data.length > 50 ? '...' : '')
      });
      
      buffer = Buffer.concat([buffer, data]);
      logger.debug('Buffer updated', { 
        bufferLength: buffer.length,
        deviceId
      });
      
      // Check if we have enough data for processing
      if (buffer.length < 8) {
        logger.debug('Insufficient data in buffer, waiting for more', { 
          bufferLength: buffer.length,
          deviceId
        });
        return;
      }
      
      // First connection - IMEI identification
      if (!deviceId && buffer.length >= 17) {
        logger.debug('Processing IMEI identification');
        const imeiLength = buffer.readUInt16BE(0);
        logger.debug('IMEI length', { imeiLength });
        
        if (imeiLength === 15 && buffer.length >= imeiLength + 2) {
          deviceId = buffer.slice(2, 17).toString();
          logger.info('Device IMEI identified', { 
            deviceId,
            clientInfo,
            connectionDuration: new Date() - connectionStartTime
          });
          
          // Send acknowledgment
          socket.write(Buffer.from([0x01]));
          logger.debug('IMEI acknowledgment sent', { deviceId });
          
          // Store device connection
          connectedDevices.set(deviceId, {
            socket,
            lastSeen: new Date(),
            address: socket.remoteAddress,
            port: socket.remotePort,
            connectedAt: connectionStartTime
          });
          
          logger.info('Device registered in connected devices', { 
            deviceId,
            totalConnectedDevices: connectedDevices.size
          });
          
          buffer = buffer.slice(17);
          logger.debug('Buffer updated after IMEI processing', { 
            bufferLength: buffer.length,
            deviceId
          });
        } else {
          logger.warn('Invalid IMEI format detected', { 
            imeiLength,
            bufferLength: buffer.length,
            clientInfo
          });
        }
      }
      
      // Process GPS data
      if (deviceId && buffer.length > 8) {
        logger.debug('Processing GPS data', { 
          deviceId,
          bufferLength: buffer.length
        });
        
        const records = TeltonikaParser.parseCodec8(buffer);
        
        if (records && records.length > 0) {
          logger.info('GPS records parsed successfully', { 
            deviceId,
            recordCount: records.length
          });
          
          // Save to database
          let savedCount = 0;
          for (const record of records) {
            try {
              const trackerData = new TrackerData({
                deviceId,
                ...record,
                rawData: buffer.toString('hex')
              });
              
              await trackerData.save();
              savedCount++;
              
              logger.info('GPS record saved to database', {
                deviceId,
                recordId: trackerData._id,
                latitude: record.latitude,
                longitude: record.longitude,
                speed: record.speed,
                timestamp: record.timestamp,
                satellites: record.satellites
              });
            } catch (saveError) {
              logger.error('Failed to save GPS record to database', {
                deviceId,
                error: saveError.message,
                record
              });
            }
          }
          
          logger.info('Database save operation completed', {
            deviceId,
            totalRecords: records.length,
            savedRecords: savedCount,
            failedRecords: records.length - savedCount
          });
          
          // Send acknowledgment (number of records processed)
          const ackBuffer = Buffer.alloc(4);
          ackBuffer.writeUInt32BE(records.length, 0);
          socket.write(ackBuffer);
          
          logger.debug('Data acknowledgment sent', { 
            deviceId,
            recordCount: records.length,
            ackHex: ackBuffer.toString('hex')
          });
          
          // Update last seen
          if (connectedDevices.has(deviceId)) {
            connectedDevices.get(deviceId).lastSeen = new Date();
            logger.debug('Device last seen updated', { deviceId });
          }
        } else {
          logger.warn('Failed to parse GPS data or no records found', { 
            deviceId,
            bufferLength: buffer.length,
            bufferHex: buffer.toString('hex').substring(0, 100) + '...'
          });
        }
        
        buffer = Buffer.alloc(0);
        logger.debug('Buffer cleared after GPS processing', { deviceId });
      }
    } catch (error) {
      logger.error('Error processing tracker data', {
        deviceId,
        clientInfo,
        error: error.message,
        stack: error.stack
      });
      buffer = Buffer.alloc(0);
    }
  });
  
  socket.on('close', (hadError) => {
    const disconnectionTime = new Date();
    const connectionDuration = disconnectionTime - connectionStartTime;
    
    logger.info('Tracker connection closed', { 
      deviceId: deviceId || 'Unknown',
      clientInfo,
      hadError,
      connectionDuration: `${Math.round(connectionDuration / 1000)}s`,
      totalConnectedDevices: connectedDevices.size
    });
    
    if (deviceId) {
      connectedDevices.delete(deviceId);
      logger.info('Device removed from connected devices', { 
        deviceId,
        remainingDevices: connectedDevices.size
      });
    }
  });
  
  socket.on('error', (error) => {
    logger.error('Socket error occurred', {
      deviceId: deviceId || 'Unknown',
      clientInfo,
      error: error.message,
      code: error.code,
      stack: error.stack
    });
    
    if (deviceId) {
      connectedDevices.delete(deviceId);
      logger.info('Device removed due to socket error', { deviceId });
    }
  });
});

// HTTP API Routes

// Get all tracker data
app.get('/api/tracker-data', async (req, res) => {
  const startTime = new Date();
  try {
    logger.info('Fetching tracker data', {
      query: req.query,
      ip: req.ip
    });

    const { deviceId, limit = 100, page = 1 } = req.query;
    const query = deviceId ? { deviceId } : {};

    logger.debug('Database query parameters', { query, limit, page });

    // Validate query parameters
    const parsedLimit = parseInt(limit);
    const parsedPage = parseInt(page);
    if (isNaN(parsedLimit) || isNaN(parsedPage) || parsedLimit <= 0 || parsedPage <= 0) {
      throw new Error('Invalid limit or page parameter');
    }

    const data = await TrackerData.find(query)
      .sort({ timestamp: -1 })
      .limit(parsedLimit)
      .skip((parsedPage - 1) * parsedLimit);

    const total = await TrackerData.countDocuments(query);

    const responseTime = new Date() - startTime;
    logger.info('Tracker data fetched successfully', {
      deviceId,
      recordCount: data.length,
      totalRecords: total,
      page: parsedPage,
      limit: parsedLimit,
      responseTime: `${responseTime}ms`
    });

    res.json({
      success: true,
      data,
      pagination: {
        page: parsedPage,
        limit: parsedLimit,
        total,
        pages: Math.ceil(total / parsedLimit)
      }
    });
  } catch (error) {
    const responseTime = new Date() - startTime;
    logger.error('Failed to fetch tracker data', {
      error: error.message,
      query: req.query,
      responseTime: `${responseTime}ms`,
      stack: error.stack
    });
    res.status(500).json({ success: false, error: error.message });
  }
});

// Get latest position (latitude and longitude only) of the most recent record
app.get('/api/tracker-data/latest-position', async (req, res) => {
  const startTime = new Date();
  try {
    logger.info('Fetching latest position', { ip: req.ip });

    const latestData = await TrackerData.findOne()
      .sort({ timestamp: -1 })
      .select('latitude longitude');

    if (!latestData) {
      logger.warn('No position data found in database');
      return res.status(404).json({ success: false, message: 'No position data found' });
    }

    const responseTime = new Date() - startTime;
    logger.info('Latest position fetched successfully', {
      latitude: latestData.latitude,
      longitude: latestData.longitude,
      responseTime: `${responseTime}ms`
    });

    res.json({
      success: true,
      latitude: latestData.latitude,
      longitude: latestData.longitude
    });
  } catch (error) {
    const responseTime = new Date() - startTime;
    logger.error('Failed to fetch latest position', {
      error: error.message,
      responseTime: `${responseTime}ms`,
      stack: error.stack
    });
    res.status(500).json({ success: false, message: error.message });
  }
});

// Get latest position for a device
app.get('/api/tracker-data/latest/:deviceId', async (req, res) => {
  try {
    const { deviceId } = req.params;
    const latestData = await TrackerData.findOne({ deviceId })
      .sort({ timestamp: -1 });
    
    if (!latestData) {
      return res.status(404).json({ success: false, message: 'Device not found' });
    }
    
    res.json({ success: true, data: latestData });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Get connected devices
app.get('/api/devices/connected', (req, res) => {
  const devices = Array.from(connectedDevices.entries()).map(([deviceId, info]) => ({
    deviceId,
    lastSeen: info.lastSeen,
    address: info.address,
    isOnline: (new Date() - info.lastSeen) < 300000 // 5 minutes
  }));
  
  res.json({ success: true, data: devices });
});

// Get device history with date range
app.get('/api/tracker-data/history/:deviceId', async (req, res) => {
  try {
    const { deviceId } = req.params;
    const { startDate, endDate, limit = 1000 } = req.query;
    
    const query = { deviceId };
    
    if (startDate || endDate) {
      query.timestamp = {};
      if (startDate) query.timestamp.$gte = new Date(startDate);
      if (endDate) query.timestamp.$lte = new Date(endDate);
    }
    
    const data = await TrackerData.find(query)
      .sort({ timestamp: 1 })
      .limit(parseInt(limit))
      .select('latitude longitude speed timestamp direction ignition');
    
    res.json({ success: true, data });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Send command to device (if supported)
app.post('/api/devices/:deviceId/command', (req, res) => {
  const { deviceId } = req.params;
  const { command } = req.body;
  
  const device = connectedDevices.get(deviceId);
  if (!device) {
    return res.status(404).json({ success: false, message: 'Device not connected' });
  }
  
  try {
    // Send SMS command format for Teltonika
    const commandBuffer = Buffer.from(command + '\r\n');
    device.socket.write(commandBuffer);
    
    res.json({ success: true, message: 'Command sent successfully' });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({
    status: 'OK',
    timestamp: new Date(),
    connectedDevices: connectedDevices.size,
    mongodb: mongoose.connection.readyState === 1 ? 'connected' : 'disconnected'
  });
});

// Error handling middleware
app.use((error, req, res, next) => {
  console.error('Error:', error);
  res.status(500).json({ success: false, error: 'Internal server error' });
});

// Start servers
const HTTP_PORT = process.env.HTTP_PORT || 3000;
const TCP_PORT = process.env.TCP_PORT || 8080;

// Start HTTP server
app.listen(HTTP_PORT, () => {
  console.log(`HTTP Server running on port ${HTTP_PORT}`);
});

// Start TCP server for tracker connections
trackerServer.listen(TCP_PORT, '0.0.0.0', () => {
  console.log(`Tracker TCP Server listening on port ${TCP_PORT}`);
});

// Graceful shutdown
process.on('SIGINT', () => {
  console.log('Shutting down servers...');
  trackerServer.close();
  mongoose.connection.close();
  process.exit(0);
});

module.exports = app;