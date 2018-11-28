import eviltransform
import pymongo
from bson.son import SON

client = pymongo.MongoClient()
db = client['hangzhou']
collection = db['roads']

# 转化经纬度坐标
def wgs2gcj(wgsLat, wgsLng):
  gcjLoc = eviltransform.wgs2gcj(wgsLat, wgsLng)
  return { 'lat': gcjLoc[0], 'lon': gcjLoc[1] }

# 处理POIs数据文件
def parsePOIs(poisFile):
  with open(poisFile, 'r') as f:
    pois = []
    ifHeader = 1
    for line in f.readlines():
      if ifHeader == 1:
        ifHeader = 0
        continue
      
      poi = line.split(',')
      gcjLoc = wgs2gcj(float(poi[3]), float(poi[2]))
      pois.append({
        'name': poi[0],
        'category': poi[1],
        'lon': gcjLoc['lon'],
        'lat': gcjLoc['lat']
      })
  return pois

# 处理位置字符串
def parseLocStr(s):
  loc = s.split(' ')
  return [ float(loc[1]), float(loc[0]) ]

# 处理路网数据文件
def parseRoads(roadsFile):
  roads = []
  with open(roadsFile, 'r') as f:
    # 两行为一条道路，进行控制
    i = -1  
    for line in f.readlines():
      i = i + 1
      if i % 2 == 0:
        continue
      else:
        id = int(i / 2)
        # 处理一个道路字符串，输出点{lat, lng}的数组
        points = list(map(parseLocStr, line.strip('\n').split(',')))
        # 将道路存入 mongo
        roads.append({
          'id': id,
          'path': {
            'type': 'LineString',
            'coordinates': points
          }
        })
  return roads

# 路网数据库构建
def insertRoadsInDB(roads):
  # 清空现有库：删除数据和索引
  collection.delete_many({})
  collection.drop_indexes()
  # 插入数据和建立索引
  collection.insert_many(roads)
  collection.create_index([('path', '2dsphere')])

# POIs和路网进行Match，输出带RoadID的POIs
def mapMatch(pois):
  for poi in pois:
    # 优化前
    result1 = db.command(SON([('geoNear', 'roads'), ('near', [poi['lon'], poi['lat']]), ('spherical', 'true')])) 
    # 优化后：更慢，有待深入探索
    # result2 = db.command(SON([('geoNear', 'roads'), ('near', [poi['lon'], poi['lat']]), ('spherical', 'true'), ('limit', 1), ('maxDistance', 100)])) 
    poi['roadid'] = result1['results'][0]['obj']['id']
  return pois

def main():
  pois = parsePOIs('data/pois.csv')
  roads = parseRoads('data/roads.txt')
  insertRoadsInDB(roads)
  pois = mapMatch(pois)

if __name__ == "__main__":
  main()