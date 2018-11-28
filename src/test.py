import pymongo
from bson.son import SON

client = pymongo.MongoClient()
db = client['hangzhou']
collection = db['roads']

# 初始化数据库
def initMongo():
  # 简单生成一些测试数据
  simpleRoadsData = [
    {
      "id": 1,
      "path": {
        "type": "LineString",
        "coordinates": [[100, 20], [101, 20]]
      }
    },
    {
      "id": 2,
      "path": {
        "type": "LineString",
        "coordinates": [[103, 20], [104, 20]]
      }
    },
    {
      "id": 3,
      "path": {
        "type": "LineString",
        "coordinates": [[105, 20], [106, 20]]
      }
    }
  ]
  # 清空现有库：删除数据和索引
  collection.delete_many({})
  collection.drop_indexes()
  # 插入数据和建立索引
  collection.insert_many(simpleRoadsData)
  collection.create_index([('path', '2dsphere')])
  
def test():
  # geoNear查询
  result = db.command(SON([('geoNear', 'roads'), ('near', [102.5, 20]), ('spherical', 'true')]))
  print(result)

def main():
  initMongo()
  test()

if __name__ == "__main__":
    main()
