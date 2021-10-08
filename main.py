from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.dialects.postgresql import UUID, ARRAY, JSONB

import json

Base = declarative_base()
engine = create_engine('postgresql://postgres:password@localhost/testdb')
Session = sessionmaker(engine)
session = Session()

class CastingArray(ARRAY):
  def bind_expression(self, bindvalue):
    return sa.cast(bindvalue, self)

class Dfs(Base):
  __tablename__ = 'dfs'

  id = Column(UUID(as_uuid=True), primary_key = True)
  keyword = Column(String(50))
  datetime = Column(DateTime())
  items = relationship("Item", back_populates="dfs", cascade="all, delete-orphan")

class Item(Base):
  __tablename__ = 'items'
  id = Column(Integer, primary_key=True, autoincrement=True)
  index = Column(Integer)
  title = Column(String(100))
  xpath = Column(String(1000))
  domain = Column(String(50))
  position = Column(String(50))
  description = Column(String(1000))
  type = Column(String(50))
  rank_absolute = Column(Integer)
  rank_group = Column(Integer)
  dfs_id = Column(UUID(as_uuid=True), ForeignKey('dfs.id'))
  dfs = relationship("Dfs", back_populates="items")
  faq_items = relationship("FaqItem", back_populates="parent_item", cascade="all, delete-orphan")

class FaqItem(Base):
  __tablename__ = 'faq_items'
  id = Column(Integer, primary_key=True, autoincrement=True)
  index = Column(Integer)
  type = Column(String(50))
  title = Column(String(100))
  description = Column(String(1000))
  parent_item_id = Column(Integer, ForeignKey('items.id'))
  parent_item = relationship("Item", back_populates="faq_items")

def load_faq(faq, db_item_id):
  index = 0
  for faq_item in faq['items']:
    db_faq_item = session.query(FaqItem).filter(FaqItem.index == index and FaqItem.parent_item_id == db_item_id).first()
    # to ensure idempotency
    if db_faq_item == None:
      db_faq_item = FaqItem(index = index, title = faq_item.get("title"), description = faq_item.get("description"),
        type = faq.get("type"), parent_item_id = db_item_id)
      session.add(db_faq_item)
      index = index + 1
  session.commit()

def load_items(items, dfs_id):
  index = 0
  for item in items:
    db_item = session.query(Item).filter(Item.index == index and Item.dfs_id == dfs_id).first()
    # to ensure idempotency
    if db_item == None:
      db_item = Item(title = item.get("title"), xpath = item.get("xpath"), domain = item.get("domain"), position = item.get("position"),
        description = item.get("description"), type = item.get("type"), rank_absolute = item.get("rank_absolute"), rank_group = item.get("rank_group"))
      session.add(db_item)

      faq = item.get('faq')
      if faq != None:
        load_faq(faq, db_item.id)
    index = index + 1
  session.commit()

def load_json_to_db():
  with open('dfs_ranking_data.json') as json_file:
    data = json.load(json_file)

    task_id = data["task_id"]
    dfs = session.query(Dfs).get({"id": task_id})
    if dfs == None:
      dfs = Dfs(id = data["task_id"], keyword = data["keyword"], datetime = data["datetime"])
      session.add(dfs)
      session.commit()
    load_items(data["items"], dfs.id)

if __name__ == '__main__':
  if not database_exists(engine.url):
    create_database(engine.url)

  Base.metadata.drop_all(engine)
  Base.metadata.create_all(engine)

  load_json_to_db()
