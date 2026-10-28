from sqlalchemy import create_engine, text

engine = create_engine("postgresql+psycopg2://postgres:iamkennyakomolafe@localhost:5432/BTCUSD_db")

with engine.begin() as connection:
    connection.execute(text('TRUNCATE TABLE "btcdata" RESTART IDENTITY;'))