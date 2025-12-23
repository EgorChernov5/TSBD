import os
from superset.app import create_app
from superset.extensions import db

app = create_app()

with app.app_context():
    from superset.models.core import Database

    DATABASE_NAME = "Clash of Clans DB"

    SQLALCHEMY_URI = (
        f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:"
        f"{os.environ['POSTGRES_PASSWORD']}@postgres/"
        f"{os.environ['CLAN_DB']}"
    )

    database = (
        db.session.query(Database)
        .filter_by(database_name=DATABASE_NAME)
        .one_or_none()
    )

    if database:
        print(f"Database already exists: {DATABASE_NAME}")
    else:
        db.session.add(
            Database(
                database_name=DATABASE_NAME,
                sqlalchemy_uri=SQLALCHEMY_URI,
                expose_in_sqllab=True,
                allow_run_async=True,
            )
        )
        db.session.commit()
        print(f"Database added: {DATABASE_NAME}")
