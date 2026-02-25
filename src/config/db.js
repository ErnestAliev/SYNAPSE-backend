const mongoose = require('mongoose');

async function connectWithLabel(uri, label, timeoutMs) {
  await mongoose.connect(uri, {
    serverSelectionTimeoutMS: timeoutMs,
  });
  console.log(`MongoDB connected (${label})`);
}

async function connectDB() {
  const isProduction = process.env.NODE_ENV === 'production';
  const localMongoUri = String(process.env.MONGO_URI_LOCAL || '').trim();
  const productionMongoUri = String(process.env.MONGO_URI || '').trim();
  const hasLocal = !isProduction && localMongoUri.length > 0;
  const hasDefault = productionMongoUri.length > 0;
  const allowLocalFallback = String(process.env.ALLOW_LOCAL_DB_FALLBACK || '').toLowerCase() === 'true';

  if (!hasLocal && !hasDefault) {
    throw new Error(
      isProduction
        ? 'MONGO_URI is not defined in environment variables'
        : 'MONGO_URI_LOCAL or MONGO_URI must be defined in environment variables',
    );
  }

  if (hasLocal) {
    try {
      await connectWithLabel(localMongoUri, 'local', 3000);
      return;
    } catch (localError) {
      console.warn(`[db] local Mongo is unavailable: ${localError.message}`);
      if (!allowLocalFallback) {
        console.error(
          '[db] fallback to MONGO_URI is disabled. Set ALLOW_LOCAL_DB_FALLBACK=true only if you really want to use production DB from local run.',
        );
        process.exit(1);
      }
      if (hasDefault) {
        console.warn('[db] falling back to MONGO_URI');
      } else {
        console.error('MongoDB connection error:', localError.message);
        process.exit(1);
      }
    }
  }

  if (hasDefault) {
    try {
      await connectWithLabel(productionMongoUri, 'default', 10000);
      return;
    } catch (defaultError) {
      console.error('MongoDB connection error:', defaultError.message);
      process.exit(1);
    }
  }
}

module.exports = connectDB;
