import json
import random

import click
from faker import Faker
from schema import Book, Paragraph, Sentence
from sqlalchemy.orm import sessionmaker

from pgsync.base import pg_engine
from pgsync.utils import get_config, show_settings


@click.command()
@click.option(
    "--config",
    "-c",
    help="Schema config",
    type=click.Path(exists=True),
)
def main(config):
    show_settings()

    config: str = get_config(config)
    documents: dict = json.load(open(config))
    engine = pg_engine(
        database=documents[0].get("database", documents[0]["index"])
    )
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    session = Session()
    faker: Faker = Faker()
    book_id = 0
    paragraph_id = 0

    for _ in range(25000):
        books = []
        paragraphs = []
        sentences = []

        for _ in range(20):
            book_id += 1
            books.append(
                Book(
                    id=book_id,
                    title=getattr(faker, 'sentence')(),
                    description=getattr(faker, 'text')(),
                )
            )
            for _ in range(random.randint(5, 10)):
                paragraph_id += 1
                paragraphs.append(
                    Paragraph(
                        id=paragraph_id,
                        book_id=book_id,
                    )
                )
                for _ in range(random.randint(10, 30)):
                    sentences.append(
                        Sentence(
                            paragraph_id=paragraph_id,
                            text=getattr(faker, 'sentence')(),
                        )
                    )

        session.bulk_save_objects(books)
        session.commit()
        session.bulk_save_objects(paragraphs)
        session.commit()
        session.bulk_save_objects(sentences)
        session.commit()


if __name__ == "__main__":
    main()
