const express = require('express');
const cors = require('cors');
const dotenv = require('dotenv');

const connectDB = require('./config/db');
const Entity = require('./models/Entity');

dotenv.config();

const app = express();
const PORT = Number(process.env.PORT) || 3001;

app.use(cors());
app.use(express.json({ limit: '10mb' }));

app.get('/api/entities', async (req, res, next) => {
  try {
    const filter = {};

    if (req.query.type) {
      filter.type = req.query.type;
    }

    const entities = await Entity.find(filter).sort({ createdAt: -1, _id: -1 });
    res.json(entities);
  } catch (error) {
    next(error);
  }
});

app.post('/api/entities', async (req, res, next) => {
  try {
    const entity = await Entity.create(req.body);
    res.status(201).json(entity);
  } catch (error) {
    next(error);
  }
});

app.put('/api/entities/:id', async (req, res, next) => {
  try {
    const updatedEntity = await Entity.findByIdAndUpdate(req.params.id, req.body, {
      new: true,
      runValidators: true,
    });

    if (!updatedEntity) {
      return res.status(404).json({ message: 'Entity not found' });
    }

    return res.json(updatedEntity);
  } catch (error) {
    return next(error);
  }
});

app.delete('/api/entities/:id', async (req, res, next) => {
  try {
    const deletedEntity = await Entity.findByIdAndDelete(req.params.id);

    if (!deletedEntity) {
      return res.status(404).json({ message: 'Entity not found' });
    }

    return res.status(204).send();
  } catch (error) {
    return next(error);
  }
});

app.use((err, req, res, next) => {
  console.error(err);

  if (err.name === 'ValidationError') {
    return res.status(400).json({ message: err.message });
  }

  if (err.name === 'CastError') {
    return res.status(400).json({ message: 'Invalid entity id' });
  }

  return res.status(500).json({ message: 'Internal server error' });
});

async function startServer() {
  await connectDB();

  app.listen(PORT, () => {
    console.log(`Backend server started on port ${PORT}`);
  });
}

startServer();
