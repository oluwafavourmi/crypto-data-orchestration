
from sqlalchemy import create_engine, text


engine = create_engine('postgresql://neondb_owner:npg_BC41ZoSjMpmr@ep-billowing-breeze-b7a8ki9q-pooler.c-13.us-east-1.aws.neon.tech/btcusd_db?sslmode=require&channel_binding=require')

with engine.begin() as connection:
    connection.execute(text('TRUNCATE TABLE "btcdata" RESTART IDENTITY;'))