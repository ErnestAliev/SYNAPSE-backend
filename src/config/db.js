const mongoose = require('mongoose');

async function connectDB() {
  const isProduction = process.env.NODE_ENV === 'production';
  const localMongoUri = String(process.env.MONGO_URI_LOCAL || '').trim();
  const productionMongoUri = String(process.env.MONGO_URI || '').trim();
  const mongoUri = !isProduction && localMongoUri ? localMongoUri : productionMongoUri;

  if (!mongoUri) {
    throw new Error(
      isProduction
        ? 'MONGO_URI is not defined in environment variables'
        : 'MONGO_URI_LOCAL or MONGO_URI must be defined in environment variables',
    );
  }

  try {
    await mongoose.connect(mongoUri);
    const mode = !isProduction && localMongoUri ? 'local' : 'default';
    console.log(`MongoDB connected (${mode})`);
  } catch (error) {
    console.error('MongoDB connection error:', error.message);
    process.exit(1);
  }
}

module.exports = connectDB;
