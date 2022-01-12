import json

import click
import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base

from pgsync.base import create_database, create_schema, pg_engine
from pgsync.constants import SCHEMA
from pgsync.helper import teardown
from pgsync.utils import get_config

Base = declarative_base()


class Book(Base):
    __tablename__ = "book"
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    title = sa.Column(sa.String, nullable=False)
    description = sa.Column(sa.String, nullable=True)
    copyright = sa.Column(sa.String, nullable=True)


class Paragraph(Base):
    __tablename__ = "paragraph"
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    book = sa.orm.relationship(Book, backref=sa.orm.backref("paragraphs"))
    book_id = sa.Column(
        sa.Integer, sa.ForeignKey(Book.id, ondelete="CASCADE")
    )


class Sentence(Base):
    __tablename__ = "sentence"
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    text = sa.Column(sa.String, nullable=True)
    paragraph = sa.orm.relationship(Paragraph, backref=sa.orm.backref("sentences"))
    paragraph_id = sa.Column(
        sa.Integer, sa.ForeignKey(Paragraph.id, ondelete="CASCADE")
    )


def setup(config=None):
    for document in json.load(open(config)):
        database = document.get("database", document["index"])
        schema = document.get("schema", SCHEMA)
        create_database(database)
        engine = pg_engine(database=database)
        create_schema(engine, schema)
        engine = engine.connect().execution_options(
            schema_translate_map={None: schema}
        )
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)


@click.command()
@click.option(
    "--config",
    "-c",
    help="Schema config",
    type=click.Path(exists=True),
)
def main(config):
    config = get_config(config)
    teardown(config=config)
    setup(config)


if __name__ == "__main__":
    main()
