import requests
import json
import sqlite3
import gc
import os
import sys
import datetime
import time
import urllib
from random import randint

proxies={"https":"http://114.255.212.17:808"}

no_cookie_headers={
	"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:57.0) Gecko/20100101 Firefox/57.0",
}



people_info_headers=[
#headers with authorization and UA
]

db=sqlite3.connect("lives.db")
cursor=db.cursor()
homepage_url='https://api.zhihu.com/lives/homefeed?includes=live'

def restart_program():
	python=sys.executable
	os.execl(python, python, * sys.argv)

class Spider(object):
	"""docstring for Spider"""
	def __init__(self):
		super(Spider, self).__init__()
		self.lives_id=[]
		self.lives_id_temp_1=[]
		self.lives_id_temp_2=[]
		self.tags_id=[]
		self.tags_id_temp=[]
		self.speaker_id=[]
		self.speakers_already_update_today=[]
		self.lives_already_update_today=[]

		sql="select tags_id from tags"
		cursor.execute(sql)
		results=cursor.fetchall()
		for result in results:
			self.tags_id.append(result[0])

		sql="select tags_id from tags_id_temp"
		cursor.execute(sql)
		results=cursor.fetchall()
		for result in results:
			self.tags_id_temp.append(result[0])

		sql="select live_id from lives"
		cursor.execute(sql)
		results=cursor.fetchall()
		for result in results:
			self.lives_id.append(result[0])



		sql="select * from lives_id_temp_1"
		cursor.execute(sql)
		results=cursor.fetchall()
		for result in results:
			self.lives_id_temp_1.append(result[0])

		sql="select * from lives_id_temp_2"
		cursor.execute(sql)
		results=cursor.fetchall()
		for result in results:
			self.lives_id_temp_2.append(result[0])

		
		sql="select speaker_id from speakers"
		cursor.execute(sql)
		results=cursor.fetchall()
		for result in results:
			self.speaker_id.append(result[0])


		#今日已更新speaker的缓存id
		sql="select speaker_id from speakers_changed_temp"
		cursor.execute(sql)
		results=cursor.fetchall()
		for result in results:
			self.speakers_already_update_today.append(result[0])


		#今日已更新live的缓存id
		sql="select live_id from lives_changed_temp"
		cursor.execute(sql)
		results=cursor.fetchall()
		for result in results:
			self.lives_already_update_today.append(result[0])



		#今天是否已更新speakers信息
		sql="select date(update_time) from speakers_update_log order by rowid desc limit 1"
		cursor.execute(sql)
		result=cursor.fetchone()
		if result is not None:
			if result[0]<time.strftime('%Y-%m-%d',time.localtime(time.time())):
				self.speakers_whether_updated_today=False
				# self.update_lives_count=0
			else:
				self.speakers_whether_updated_today=True
				# self.update_lives_count=result[1]
		else:
			self.speakers_whether_updated_today=False
			# self.update_lives_count=0


		#今天是否已更新lives信息
		sql="select date(update_time) from lives_update_log order by rowid desc limit 1"
		cursor.execute(sql)
		result=cursor.fetchone()
		if result is not None:
			if result[0]<time.strftime('%Y-%m-%d',time.localtime(time.time())):
				self.lives_whether_updated_today=False
			else:
				self.lives_whether_updated_today=True
		else:
			self.lives_whether_updated_today=False


		# sql="select speaker_id,date(record_time) from speakers_changed order by rowid desc limit 3000"
		# cursor.execute(sql)
		# results=cursor.fetchall()
		# for result in results:
		# 	if result[1]=time.strftime('%Y-%m-%d',time.localtime(time.time())):
		# 		self.speakers_already_update_today.append(result[0])




	def insert_new_speaker(self,data):

		# if not self.lives_whether_updated_today and (time.strftime('%H:%M',time.localtime(time.time()))>time.strftime('%H:%M',(2019,2,1,22,20,0,0,0,0)) and time.strftime('%H:%M',time.localtime(time.time()))<time.strftime('%H:%M',(2019,2,1,23,40,0,0,0,0))):
		if data['speaker']['member']['id'] not in self.speaker_id:
			sql="insert into speakers values('%s','%s','%s')"%(data['speaker']['member']['id'],data['speaker']['member']['url_token'],data['speaker']['member']['name'])
			db.execute(sql)
			self.speaker_id.append(data['speaker']['member']['id'])
		if 'cospeakers' in data.keys():
			for cospeaker in data['cospeakers']:
				if cospeaker['member']['id'] not in self.speaker_id:
					sql="insert into speakers values('%s','%s','%s')"%(cospeaker['member']['id'],cospeaker['member']['url_token'],cospeaker['member']['name'])
					db.execute(sql)
					self.speaker_id.append(cospeaker['member']['id'])




	def get_new_lives_from_today(self):
		try:
			tags_url='https://api.zhihu.com/lives/tags'
			results=json.loads(urllib.request.urlopen(tags_url).read().decode('utf8'))['data']
			for result in results:
				for tag in result['data']:
					if tag['id'] not in self.tags_id:
						sql="insert into tags values(%d,'%s')"%(tag['id'],tag['short_name'])
						db.execute(sql)
						self.tags_id.append(tag['id'])
						db.commit()


			for tag_id in self.tags_id:
				if tag_id not in self.tags_id_temp:
					url="https://api.zhihu.com/lives/ongoing?tags=%d"%tag_id
					datas=json.loads(urllib.request.urlopen(url).read().decode('utf8'))
					while not datas['paging']['is_end'] and 'error' not in datas.keys():
						url=datas['paging']['next']
						for data in datas['data']:
							if data['id'] not in self.lives_id_temp_1:
								sql_1="insert into lives_id_temp_1 values('%s')"%data['id']
								cursor.execute(sql_1)
								self.lives_id_temp_1.append(data['id'])

								sql_2="select changed_price from price where live_id='%s' order by rowid desc limit 1"%data['id']
								cursor.execute(sql_2)
								price_result=cursor.fetchone()
								if price_result is not None:
									price_last_time=price_result[0]
									if price_last_time!=data['fee']['amount']:
										sql="insert into price values('%s',datetime('now','localtime'),%d)"%(data['id'],data['fee']['amount'])
										db.execute(sql)
								else:
									sql="insert into price values('%s',datetime('now','localtime'),%d)"%(data['id'],data['fee']['amount'])
									db.execute(sql)

								self.insert_new_speaker(data)


								# if self.lives_update_count and (time.strftime('%H:%M',time.localtime(time.time()))>time.strftime('%H:%M',(2019,2,1,22,20,0,0,0,0)) and time.strftime('%H:%M',time.localtime(time.time()))<time.strftime('%H:%M',(2019,2,1,23,40,0,0,0,0))):
									
								# 	sql_3="insert into lives_changed values('%s',%d,%d,'%s','%s','%s',%d,'%s',datetime('now','localtime'))"%(data['id'],data['fee']['amount'],data['seats']['taken'],data['purchasable'],data['buyable'],data['status'],data['liked_num'],data['in_promotion'])
									
								# 	db.execute(sql_3)


								if data['id'] not in self.lives_id:
									sql_1="insert into lives_id values('%s')"%data['id']
									sql_2="insert into lives values('%s',%d,%d,'%s',%d,'%s',%d)"%(data['id'],data['created_at'],data['ends_at'],data['speaker']['member']['id'],data['fee']['original_price'],data['speaker']['member']['url_token'],0)
									db.execute(sql_1)
									db.execute(sql_2)
									self.lives_id.append(data['id'])
								db.commit()
						datas=json.loads(urllib.request.urlopen(url).read().decode('utf8'))


					if 'error' not in datas.keys():
						for data in datas['data']:

							if data['id'] not in self.lives_id_temp_1:
								sql_1="insert into lives_id_temp_1 values('%s')"%data['id']
								cursor.execute(sql_1)
								self.lives_id_temp_1.append(data['id'])

								sql_2="select changed_price from price where live_id='%s' order by rowid desc limit 1"%data['id']
								
								cursor.execute(sql_2)
								price_result=cursor.fetchone()
								if price_result is not None:
									price_last_time=price_result[0]
									if price_last_time!=data['fee']['amount']:
										sql="insert into price values('%s',datetime('now','localtime'),%d)"%(data['id'],data['fee']['amount'])
										db.execute(sql)
								else:
									sql="insert into price values('%s',datetime('now','localtime'),%d)"%(data['id'],data['fee']['amount'])
									db.execute(sql)



								self.insert_new_speaker(data)



								# if not self.lives_whether_updated_today and (time.strftime('%H:%M',time.localtime(time.time()))>time.strftime('%H:%M',(2019,2,1,22,20,0,0,0,0)) and time.strftime('%H:%M',time.localtime(time.time()))<time.strftime('%H:%M',(2019,2,1,23,40,0,0,0,0))):
									
								# 	sql_3="insert into lives_changed values('%s',%d,%d,'%s','%s','%s',%d,'%s',datetime('now','localtime'))"%(data['id'],data['fee']['amount'],data['seats']['taken'],data['purchasable'],data['buyable'],data['status'],data['liked_num'],data['in_promotion'])
								# 	db.execute(sql_3)

								if data['id'] not in self.lives_id:
									sql_1="insert into lives_id values('%s')"%data['id']
									sql_2="insert into lives values('%s',%d,%d,'%s',%d,'%s',%d)"%(data['id'],data['created_at'],data['ends_at'],data['speaker']['member']['id'],data['fee']['original_price'],data['speaker']['member']['url_token'],0)
									db.execute(sql_1)
									db.execute(sql_2)
									self.lives_id.append(data['id'])
								db.commit()







					url="https://api.zhihu.com/lives/ended?tags=%d"%tag_id
					datas=json.loads(urllib.request.urlopen(url).read().decode('utf8'))
					while not datas['paging']['is_end'] and 'error' not in datas.keys():
						url=datas['paging']['next']
						for data in datas['data']:


							if data['id'] not in self.lives_id_temp_1:
								sql_1="insert into lives_id_temp_1 values('%s')"%data['id']
								cursor.execute(sql_1)
								self.lives_id_temp_1.append(data['id'])

								sql_2="select changed_price from price where live_id='%s' order by rowid desc limit 1"%data['id']
								cursor.execute(sql_2)
								price_result=cursor.fetchone()
								if price_result is not None:
									price_last_time=price_result[0]
									if price_last_time!=data['fee']['amount']:
										sql="insert into price values('%s',datetime('now','localtime'),%d)"%(data['id'],data['fee']['amount'])
										db.execute(sql)
								else:
									sql="insert into price values('%s',datetime('now','localtime'),%d)"%(data['id'],data['fee']['amount'])
									db.execute(sql)



								self.insert_new_speaker(data)

								# if not self.lives_whether_updated_today and (time.strftime('%H:%M',time.localtime(time.time()))>time.strftime('%H:%M',(2019,2,1,22,20,0,0,0,0)) and time.strftime('%H:%M',time.localtime(time.time()))<time.strftime('%H:%M',(2019,2,1,23,40,0,0,0,0))):
									
								# 	sql_3="insert into lives_changed values('%s',%d,%d,'%s','%s','%s',%d,'%s',datetime('now','localtime'))"%(data['id'],data['fee']['amount'],data['seats']['taken'],data['purchasable'],data['buyable'],data['status'],data['liked_num'],data['in_promotion'])
								# 	db.execute(sql_3)


								if data['id'] not in self.lives_id:
									sql_1="insert into lives_id values('%s')"%data['id']
									sql_2="insert into lives values('%s',%d,%d,'%s',%d,'%s',%d)"%(data['id'],data['created_at'],data['ends_at'],data['speaker']['member']['id'],data['fee']['original_price'],data['speaker']['member']['url_token'],0)
									db.execute(sql_1)
									db.execute(sql_2)
									self.lives_id.append(data['id'])
								db.commit()
						datas=json.loads(urllib.request.urlopen(url).read().decode('utf8'))


					if 'error' not in datas.keys():
						for data in datas['data']:


							if data['id'] not in self.lives_id_temp_1:
								sql_1="insert into lives_id_temp_1 values('%s')"%data['id']
								cursor.execute(sql_1)
								self.lives_id_temp_1.append(data['id'])

								sql_2="select changed_price from price where live_id='%s' order by rowid desc limit 1"%data['id']
								cursor.execute(sql_2)
								price_result=cursor.fetchone()
								if price_result is not None:
									price_last_time=price_result[0]
									if price_last_time!=data['fee']['amount']:
										sql="insert into price values('%s',datetime('now','localtime'),%d)"%(data['id'],data['fee']['amount'])
										db.execute(sql)
								else:
									sql="insert into price values('%s',datetime('now','localtime'),%d)"%(data['id'],data['fee']['amount'])
									db.execute(sql)



								self.insert_new_speaker(data)


								# if not self.lives_whether_updated_today and (time.strftime('%H:%M',time.localtime(time.time()))>time.strftime('%H:%M',(2019,2,1,22,20,0,0,0,0)) and time.strftime('%H:%M',time.localtime(time.time()))<time.strftime('%H:%M',(2019,2,1,23,40,0,0,0,0))):
									
								# 	sql_3="insert into lives_changed values('%s',%d,%d,'%s','%s','%s',%d,'%s',datetime('now','localtime'))"%(data['id'],data['fee']['amount'],data['seats']['taken'],data['purchasable'],data['buyable'],data['status'],data['liked_num'],data['in_promotion'])
								# 	db.execute(sql_3)

								if data['id'] not in self.lives_id:
									sql_1="insert into lives_id values('%s')"%data['id']
									sql_2="insert into lives values('%s',%d,%d,'%s',%d,'%s',%d)"%(data['id'],data['created_at'],data['ends_at'],data['speaker']['member']['id'],data['fee']['original_price'],data['speaker']['member']['url_token'],0)
									db.execute(sql_1)
									db.execute(sql_2)
									self.lives_id.append(data['id'])
								db.commit()




					sql="insert into tags_id_temp values(%d)"%tag_id
					db.execute(sql)
					db.commit()
					self.tags_id_temp.append(tag_id)


		except:
			info=sys.exc_info()
			print(info[0],':',info[1])
			db.close()
			del self.lives_id
			del self.lives_id_temp_1
			del self.lives_id_temp_2
			del self.tags_id_temp
			del self.tags_id
			print(gc.collect())
			print('------------restart in get new lives-----------')
			restart_program()





	def get_new_listeners(self):

		try:
			while self.lives_id:
				if self.lives_id[0] not in self.lives_id_temp_2:

					listeners=[]
					sql="select listener_id from listeners where lives_id='%s'"%self.lives_id[0]
					cursor.execute(sql)
					results=cursor.fetchall()
					for result in results:
						listeners.append(result[0])

					sql="select seats_taken from lives where live_id='%s'"%self.lives_id[0]
					cursor.execute(sql)
					result=cursor.fetchone()
					listener_nums_last_time=result[0]

					sql_1="insert into lives_id_temp_2 values('%s')"%self.lives_id[0]
					db.execute(sql_1)
					self.lives_id_temp_2.append(self.lives_id[0])

					url='https://api.zhihu.com/lives/%s/members?limit=10&offset=%d'%(self.lives_id[0],listener_nums_last_time//10*10)
					listeners_data=json.loads(urllib.request.urlopen(url).read().decode('utf8'))

					while not listeners_data['paging']['is_end']:
						next_url=listeners_data['paging']['next']
						for listener in listeners_data['data']:
							if listener['member']['id'] not in listeners and listener['member']['id']!='0':
								listener_nums_last_time=listener_nums_last_time+1
								listeners.append(listener['member']['id'])				
								sql="insert into listeners values('%s','%s','%s',%d,datetime('now','localtime'))"%(self.lives_id[0],listener['member']['id'],listener['member']['url_token'],listener['badge']['id'])
								db.execute(sql)
						listeners_data=json.loads(urllib.request.urlopen(next_url).read().decode('utf8'))

					for listener in listeners_data['data']:
						if listener['member']['id'] not in listeners and listener['member']['id']!='0':
							listener_nums_last_time=listener_nums_last_time+1
							listeners.append(listener['member']['id'])
							sql="insert into listeners values('%s','%s','%s',%d,datetime('now','localtime'))"%(self.lives_id[0],listener['member']['id'],listener['member']['url_token'],listener['badge']['id'])
							db.execute(sql)

					sql_2="update lives set seats_taken=%d where live_id='%s'"%(listener_nums_last_time,self.lives_id[0])
					db.execute(sql_2)
					
					db.commit()
					del listeners
					del listener_nums_last_time
					gc.collect()

				self.lives_id.pop(0)
		except:
			info=sys.exc_info()
			print(info[0],':',info[1])
			db.close()
			del self.lives_id
			del self.lives_id_temp_2
			del listeners
			print(gc.collect())
			print('------------restart in get new listeners-----------')
			restart_program()



	def get_rest_lives_change(self):
		try:
			if not self.lives_whether_updated_today and (time.strftime('%H:%M',time.localtime(time.time()))>time.strftime('%H:%M',(2019,2,1,2,30,0,0,0,0)) and time.strftime('%H:%M',time.localtime(time.time()))<time.strftime('%H:%M',(2019,2,1,6,0,0,0,0,0))):
				for live_id in self.lives_id:
					if live_id not in self.lives_already_update_today:

						# print(live_id)
						url="https://api.zhihu.com/lives/%s"%live_id
						data=requests.get(url,headers=no_cookie_headers).json()

						# print(live_id)
						if live_id not in self.lives_id_temp_1:
							sql_1="insert into lives_id_temp_1 values('%s')"%live_id
							cursor.execute(sql_1)
							self.lives_id_temp_1.append(live_id)


						if 'error' not in data.keys():
							sql_2="select changed_price from price where live_id='%s' order by rowid desc limit 1"%live_id
							cursor.execute(sql_2)
							price_result=cursor.fetchone()
							if price_result is not None:
								price_last_time=price_result[0]
								if price_last_time!=data['fee']['amount']:
									sql="insert into price values('%s',datetime('now','localtime'),%d)"%(data['id'],data['fee']['amount'])
									db.execute(sql)
							else:
								sql="insert into price values('%s',datetime('now','localtime'),%d)"%(data['id'],data['fee']['amount'])
								db.execute(sql)

							self.insert_new_speaker(data)

							sql_3="insert into lives_changed values('%s',%d,%d,'%s','%s','%s',%d,'%s',datetime('now','localtime'))"%(live_id,data['fee']['amount'],data['seats']['taken'],data['purchasable'],data['buyable'],data['status'],data['liked_num'],data['in_promotion'])
							db.execute(sql_3)

							sql_4="insert into lives_changed_temp values('%s')"%live_id
							db.execute(sql_4)

							db.commit()

						self.lives_already_update_today.append(live_id)

				sql="insert into lives_update_log values(datetime('now','localtime'))"
				db.execute(sql)
				db.commit()



		except:
			info=sys.exc_info()
			print(info[0],':',info[1])
			db.close()
			del self.lives_id
			del self.lives_id_temp_1
			del self.lives_id_temp_2
			del self.tags_id
			del self.tags_id_temp
			del self.speaker_id
			del self.speakers_already_update_today
			del self.lives_already_update_today
			del self.lives_whether_updated_today
			del self.speakers_whether_updated_today
			print(gc.collect())
			print('------------restart in get rest lives changed-----------')
			restart_program()


	def update_insert_speakers_info(self):
		try:
			if not self.speakers_whether_updated_today  and (time.strftime('%H:%M',time.localtime(time.time()))>time.strftime('%H:%M',(2019,2,1,2,30,0,0,0,0)) and time.strftime('%H:%M',time.localtime(time.time()))<time.strftime('%H:%M',(2019,2,1,6,0,0,0,0,0))):
				for speaker_id in self.speaker_id:
					if speaker_id not in self.speakers_already_update_today:
						print(speaker_id)
						url="https://api.zhihu.com/people/%s"%speaker_id
						index=randint(0,len(people_info_headers)-1)
						headers=people_info_headers[index]
						data=requests.get(url,headers=headers).json()
						while 'error' in data.keys():
							people_info_headers.pop(index)
							index=randint(0,len(people_info_headers))
							headers=people_info_headers[index]
							data=requests.get(url,headers=headers,proxies=proxies).json()
						sql_1="insert into speakers_changed values('%s',%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,datetime('now','localtime'))"%(speaker_id,data['shared_count'],data['voteup_count'],data['favorited_count'],data['follower_count'],data['thanked_count'],data['hosted_live_count'],data['answer_count'],data['columns_count'],data['articles_count'],data['favorite_count'],data['following_topic_count'],data['question_count'],data['pins_count'],data['following_count'],data['following_columns_count'],data['following_question_count'],data['following_favlists_count'])
						db.execute(sql_1)
						sql_2="insert into speakers_changed_temp values('%s')"%speaker_id
						db.execute(sql_2)
						self.speakers_already_update_today.append(speaker_id)
						db.commit()

				sql="insert into speakers_update_log values(datetime('now','localtime'))"
				db.execute(sql)
				db.commit()

		except:
			info=sys.exc_info()
			print(info[0],':',info[1])
			db.close()
			del self.speakers_already_update_today
			del self.speaker_id
			print(gc.collect())
			print('------------restart in update insert speakers info-----------')
			restart_program()
		

	def delete_all_temp(self):
		del self.lives_id
		del self.lives_id_temp_1
		del self.lives_id_temp_2
		del self.tags_id
		del self.tags_id_temp
		del self.speaker_id
		del self.speakers_already_update_today
		del self.lives_already_update_today
		del self.lives_whether_updated_today
		del self.speakers_whether_updated_today

		sql_1="delete from lives_id_temp_1"
		sql_2="delete from lives_id_temp_2"
		sql_3="delete from tags_id_temp"
		sql_4="delete from speakers_changed_temp"
		sql_5="delete from lives_changed_temp"
		db.execute(sql_1)
		db.execute(sql_2)
		db.execute(sql_3)
		db.execute(sql_4)
		db.execute(sql_5)
		db.commit()







if __name__ == '__main__':
	# spider=Spider()
	i=1
	while True:
		start_time=datetime.datetime.now()
		print('第{}次运行开始，开始时间为{}'.format(i,start_time))
		spider=Spider()
		try:
			spider.get_new_lives_from_today()
			spider.get_rest_lives_change()
			spider.get_new_listeners()
			spider.update_insert_speakers_info()
			spider.delete_all_temp()

			end_time=datetime.datetime.now()
			delta=end_time-start_time
			delta_gmtime=time.gmtime(delta.total_seconds())
			duration_str=time.strftime("%H:%M:%S",delta_gmtime)

			print('第{}次运行结束,开始时间为{},结束时间为{},此次运行时长为{}'.format(i,start_time,end_time,duration_str))
		except:
			info=sys.exc_info()
			print(info[0],':',info[1])
			db.close()
			del spider
			print(gc.collect())
			print('------------restart in main-----------')
			restart_program()
		del spider
		i=i+1
		print(gc.collect())
		print('------------begin again-----------')
		# db.close()


