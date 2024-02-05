# youtube project
from pprint import pprint
import googleapiclient.discovery
from pymongo.server_api import ServerApi
import pymongo
from pymongo.mongo_client import MongoClient
import mysql.connector
import streamlit as st
import pandas as pd

#API connect

api_service_name = "youtube"
api_version = "v3"
client_secrets_file = "YOUR_CLIENT_SECRET_FILE.json"
api_key="AIzaSyBluDCPLqTyA8tKTVOgj5HnUp7XkMYW9Pw"
youtube = googleapiclient.discovery.build(
    api_service_name, api_version, developerKey=api_key)
request = youtube.channels().list(
    part="snippet,contentDetails,statistics",
    id="UCZCaRGGPtHFtp7X3FxtNKhA")
response = request.execute()

##channel details
def channel_info(channel_id):
  request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id)
  response = request.execute()
  for i in range(0,len(response["items"])):
    chanel_info=dict(channel_id=response['items'][0]['id'],
    time_stamp=response['items'][0]['snippet']['publishedAt'],
    channel_name=response['items'][0]['snippet']['title'],
    channel_discription=response['items'][0]['snippet']['localized']['description'],
    subscriber=response['items'][0]['statistics']['subscriberCount'],
    videocount=response['items'][0]['statistics']['videoCount'],
    viewcount=response['items'][0]['statistics']['viewCount'],
    playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads'])
  return chanel_info

##playlist details


def playlist_info(channel_id):
        playlist_details=[]
        
        next_page_token=None
        next_page=True
        while next_page:
                request = youtube.playlists().list(
                part="snippet,contentDetails",
                channelId=channel_id,
                maxResults=25,
                pageToken=next_page_token
                )
                response4 = request.execute()

                for items in response4['items']:
                        data=dict(playlist_id=items['id'],
                                title=items['snippet']['title'],
                                channel_id=items['snippet']['channelId'],
                                channel_publish=items['snippet']['publishedAt'],
                                video_count=items['contentDetails']['itemCount'] )
                        playlist_details.append(data)
                next_page_token=response4.get('nextpageToken')
                if next_page_token is None:
                        next_page=False
                
        return playlist_details
                
##video Id's

def channel_video_id(channel_id):
    video_ids=[]
    request = youtube.channels().list(
        part="contentDetails",
        id=channel_id)
    response= request.execute()
    playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token=None
    while True:
        
        request = youtube.playlistItems().list(
                    part="snippet,contentDetails",
                    maxResults=50,
                    playlistId=playlist_id,
                    pageToken=next_page_token
                )
        response= request.execute()
    
        for i in range(len(response['items'])):
            video_ids.append(response['items'][i]['contentDetails']['videoId'])
            next_page_token=response.get('nextPageToken')
        
        if next_page_token is None:
            break
    return video_ids

##video details

def get_video_info(video_ids):
    video_data=[]
    
    for video_id in video_ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response2 = request.execute()

        for item in response2['items']:
            data=dict(channel_name=item['snippet']['channelTitle'],
                    channel_id=item['snippet']['channelId'],
                    video_id=item['id'],
                    title=item['snippet']['title'],
                    tag=item['snippet'].get('tags'),
                    thumn=item['snippet']['thumbnails']['default']['url'],
                    discrip=item['snippet']['description'],
                    upload_date=item['snippet']['publishedAt'],
                    vid_lenth=item['contentDetails']['duration'],
                    views=item['statistics'].get('viewCount'),
                    likes=item['statistics'].get('likeCount'),
                    comments_count=item['statistics'].get('commentCount'),
                    fav_count=item['statistics']['favoriteCount'],
                    definition=item['contentDetails']['definition'],
                    captions=item['contentDetails']['caption'])
            video_data.append(data)
    return video_data

    
## comment details

def comment_info(Video_Ids):
        commend_data=[]
    
        try:
            for video_id in Video_Ids:
                request = youtube.commentThreads().list(
                        part="snippet,replies",
                        videoId=video_id,
                        maxResults=50
                    )
                response3 = request.execute()
                
                for item in response3['items']:
                    comment=dict(comment_id=item['snippet']['topLevelComment']['id'],
                            comment_video_id=item['snippet']['topLevelComment']['snippet']['videoId'],
                            comment_text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            comment_author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            comment_published=item['snippet']['topLevelComment']['snippet']['updatedAt']
                            )
                    commend_data.append(comment)
        except:
            pass
        return commend_data

##MongoDB connect
client = MongoClient("mongodb+srv://Mksumith120195:Mksumithabc123@cluster0.n7sdrct.mongodb.net/?retryWrites=true&w=majority")
database=client["youtube"]
collection=database["mk_channel_details"]
# 1, The Urban Fight - UCMSI1Ck1mJOaxxwJ0bzrYhQ
# 2,tech hub - UCQT36gti8cBe48VM6pl75Ew
# 3,jerry lessons - UCyqDTGmtR6abt_Fb4XBaIhw
def channel_details(channel_id):
    channel_details=channel_info(channel_id)
    playlist_details=playlist_info(channel_id)
    channelv_ids=channel_video_id(channel_id)
    video_details=get_video_info(channelv_ids)
    comment_details=comment_info(channelv_ids)

    collection=database["mk_channel_details"]
    collection.insert_one({"channel_information":channel_details,"playlist_information":playlist_details,"video_informations":video_details,"comment_information":comment_details,})
    return "upload completed successfully"
###channel table
def channel_table():
  import mysql.connector
  mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="youtube",
    port="3306")

  print(mydb)
  mycursor = mydb.cursor(buffered=True)
  drop_query='''drop table if exists channels'''
  mycursor.execute(drop_query)
  mydb.commit()
  try:
      create_query='''create table if not exists channels(channel_id varchar(80) primary key,
                                                          time_stamp varchar(100),
                                                          channel_name varchar(100),
                                                          channel_discription text,
                                                          subscriber bigint,
                                                          videocount int,
                                                          viewcount bigint,
                                                          playlist_id varchar(100))'''
      mycursor.execute(create_query)
      mydb.commit()
  except:
      print("channel table already created")
  channel_list=[]
  db=client["youtube"]
  collection=db["mk_channel_details"]
  for ch_data in collection.find({},{"_id":0,"channel_information":1}):
      channel_list.append(ch_data["channel_information"])
  df=pd.DataFrame(channel_list)
  for index,row in df.iterrows():
    insert_query='''insert into channels(channel_id,time_stamp,channel_name,channel_discription,subscriber,videocount,viewcount,playlist_id)
    values(%s,%s,%s,%s,%s,%s,%s,%s)'''
    values=(row["channel_id"],
            row["time_stamp"],
            row["channel_name"],
            row["channel_discription"],
            row["subscriber"],
            row["videocount"],
            row["viewcount"],
            row["playlist_id"]
            )
    try:
        mycursor.execute(insert_query,values)
        mydb.commit()
    except:
        print("channel value alreay inserted")
  return "channel table creaed sucessfully"

###Playlist table
def playlist_table():
  mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="youtube",
    port="3306")
  print(mydb)
  mycursor = mydb.cursor(buffered=True)
  drop_query='''drop table if exists test'''
  mycursor.execute(drop_query)
  mydb.commit()
  try:
      create_query='''create table if not exists playlist(playlist_id varchar(80) primary key,
                                                          title varchar(100),
                                                          channel_id varchar(100),
                                                          channel_publish timestamp,
                                                          video_count int)'''
      mycursor.execute(create_query)
      mydb.commit()
  except:
      print("playlist table already created")
  db=client["youtube"]
  collection=db["mk_channel_details"]
  play_list=[]
  for playlist_data in collection.find({},{"_id":0,"playlist_information":1}):
    
    for i in range(len(playlist_data["playlist_information"])):
        play_list.append(playlist_data["playlist_information"][i])    
  df1=pd.DataFrame(play_list)
  for index,row in df1.iterrows():
      insert_query='''insert into playlist(playlist_id,title,channel_id,channel_publish,video_count)
      values(%s,%s,%s,%s,%s)'''
      values=(row['playlist_id'],
              row['title'],
              row['channel_id'],
              row['channel_publish'],
              row['video_count']
              )
      try:
          mycursor.execute(insert_query, values)
          mydb.commit()
      except mysql.connector.IntegrityError as e:
          # Handle duplicate entry error here (skip or update existing record)
          print(f"Duplicate entry: {e}")

  return "playlist table created sucessfully"


### video table
def video_table():
    mydb = mysql.connector.connect(host="localhost",
                                        user="root",
                                        password="",
                                        database="youtube",
                                        port="3306")
    print(mydb)
    mycursor = mydb.cursor(buffered=True)
    drop_query='''drop table if exists videos'''
    mycursor.execute(drop_query)
    mydb.commit()
    try:
        create_query='''create table if not exists videos(channel_name varchar(80),
                                                            channel_id varchar(100),
                                                            video_id varchar(100) primary key,
                                                            title varchar(150),
                                                            tag varchar(30),
                                                            thumn varchar(200),
                                                            discrip varchar(100),
                                                            upload_date timestamp,
                                                            vid_lenth duration,
                                                            views bigint,
                                                            likes bigint,
                                                            comments_count int,
                                                            fav_count int,
                                                            definition varchar(10),
                                                            captions varchar(50)
                                                            )'''
        mycursor.execute(create_query)
        mydb.commit()
    except:
        print("vide table already created")
    db=client["youtube"]
    collection=db["mk_channel_details"]
    video_list=[]
    for video_data in collection.find({},{"_id":0,"video_informations":1}):
        for i in range(len(video_data["video_informations"])):
            video_list.append(video_data["video_informations"][i])  
    df2=pd.DataFrame(video_list)
    
    for index, row in df2.iterrows():
        # Convert 'tag' column from list to a string
        row['tag'] = ', '.join(row['tag']) if isinstance(row['tag'], list) else row['tag']

        insert_query = '''insert into videos(channel_name,channel_id,video_id,title,tag,thumn,discrip,upload_date,vid_lenth,views,likes,comments_count,fav_count,definition,captions)
        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

        values = (
            row['channel_name'],
            row['channel_id'],
            row['video_id'],
            row['title'],
            row['tag'],
            row['thumn'],
            row['discrip'],
            row['upload_date'],
            row['vid_lenth'],
            row['views'],
            row['likes'],
            row['comments_count'],
            row['fav_count'],
            row['definition'],
            row['captions']
        )

        mycursor.execute(insert_query, values)
        mydb.commit()
    return "video table created successfully"

### comment table
def comment_table():
  mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="youtube",
    port="3306"
  )
  mycursor = mydb.cursor(buffered=True)
  drop_query='''drop table if exists comments'''
  mycursor.execute(drop_query)
  mydb.commit()
  try:
          create_query = '''CREATE TABLE if not exists comments(comment_id varchar(100) primary key,
                        comment_video_id varchar(80),
                        comment_text text, 
                        comment_author varchar(150),
                        comment_published timestamp)'''
          mycursor.execute(create_query)
          mydb.commit()
          
  except:
      print("command table already created")
  db=client["youtube"]
  collection=db["mk_channel_details"]
  comment_list=[]
  for comment_data in collection.find({},{"_id":0,"comment_information":1}):
    
    for i in range(len(comment_data["comment_information"])):
        comment_list.append(comment_data["comment_information"][i])  
    df3=pd.DataFrame(comment_list)
    for index, row in df3.iterrows():
      insert_query = '''INSERT INTO comments(comment_id,
                        comment_video_id,
                        comment_text, 
                        comment_author,
                        comment_published)
      VALUES(%s,%s,%s,%s,%s)'''

      values = (
          row['comment_id'],
          row['comment_video_id'],
          row['comment_text'],
          row['comment_author'],
          row['comment_published']
      )

      try:
          mycursor.execute(insert_query, values)
          mydb.commit()
      except mysql.connector.IntegrityError as e:
          # Handle duplicate entry error here (skip or update existing record)
          print(f"Duplicate entry: {e}")
  return 'comment table created successfully'

#### table creation on SQL
def table():
    channel_table()
    playlist_table()
    video_table()
    comment_table()
    return "tables created sucessfully"

def show_channels_table():
    channel_list = []
    client = MongoClient("mongodb+srv://Mksumith120195:Mksumithabc123@cluster0.n7sdrct.mongodb.net/?retryWrites=true&w=majority")
    database = client["youtube"]
    collection = database["mk_channel_details"]
    for ch_data in collection.find({}, {"_id": 0, "channel_information": 1}):
        channel_list.append(ch_data["channel_information"])
    channels_table = st.dataframe(channel_list)
    return  channels_table

def show_playlists_table():
    play_list=[]
    client = MongoClient("mongodb+srv://Mksumith120195:Mksumithabc123@cluster0.n7sdrct.mongodb.net/?retryWrites=true&w=majority")
    database=client["youtube"]
    collection=database["mk_channel_details"]

    for playlist_data in collection.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(playlist_data["playlist_information"])):
            play_list.append(playlist_data["playlist_information"][i])    
    playlists_table = st.dataframe(play_list)
    return playlists_table

def show_videos_table():
    video_list=[]
    client = MongoClient("mongodb+srv://Mksumith120195:Mksumithabc123@cluster0.n7sdrct.mongodb.net/?retryWrites=true&w=majority")
    database=client["youtube"]
    collection=database["mk_channel_details"]

    for video_data in collection.find({},{"_id":0,"video_informations":1}):
        for i in range(len(video_data["video_informations"])):
            video_list.append(video_data["video_informations"][i])  
    videos_table = st.dataframe(video_list)
    return videos_table

def show_comments_table():
    comment_list=[]
    client = MongoClient("mongodb+srv://Mksumith120195:Mksumithabc123@cluster0.n7sdrct.mongodb.net/?retryWrites=true&w=majority")
    database=client["youtube"]
    collection=database["mk_channel_details"]
    for comment_data in collection.find({},{"_id":0,"comment_information":1}):
            for i in range(len(comment_data["comment_information"])):
                comment_list.append(comment_data["comment_information"][i])
    comments_table = st.dataframe(comment_list)
    return comments_table

#####Streamlit part
with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("SKILL TAKE AWAY")
    st.caption('Python scripting')
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption(" Data Managment using MongoDB and SQL")
    
channel_id = st.text_input("Enter the Channel id")
channels = channel_id.split(',')
channels = [ch.strip() for ch in channels if ch]

if st.button("Collect and Store data"):
    for channel in channels:
        ch_ids = []
        db = client["Youtube_data"]
        coll1 = db["channel_details"]
        for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
            ch_ids.append(ch_data["channel_information"]["Channel_Id"])
        if channel in ch_ids:
            st.success("Channel details of the given channel id: " + channel + " already exists")
        else:
            output = channel_details(channel)
            st.success(output)
            
if st.button("Migrate to SQL"):
    display = table()
    st.success(display)
    
show_table = st.radio("SELECT THE TABLE FOR VIEW",(":green[channels]",":orange[playlists]",":red[videos]",":blue[comments]"))

if show_table == ":green[channels]":
    show_channels_table()
elif show_table == ":orange[playlists]":
    show_playlists_table()
elif show_table ==":red[videos]":
    show_videos_table()
elif show_table == ":blue[comments]":
    show_comments_table()
        
#######sql connection
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="youtube",
    port="3306")
mycursor = mydb.cursor(buffered=True)

question=st.selectbox('select your question',(  "1. All the videos and channels",
                                            "2. Channels with most number of videos",
                                            "3. Top 10 Most viewed Video",
                                            "4. Comments in each video",
                                            "5. Videos with highest likes",
                                            "6. Likes of all videos",
                                            "7. views of each channels",
                                            "8. Videos publised in the year of 2022",
                                            "9. Average duration of all videos in each channel",
                                            "10. Videos with highesdt number of comments"))

if question =="1. All the videos and channels":
    query1='''select title as videos,channel_name as channelname from videos'''
    mycursor.execute(query1)
    mydb.commit
    t1=mycursor.fetchall()
    df=pd.DataFrame(t1,columns=["video title","channel name"])
    st.write(df)
elif question =="2. Channels with most number of videos":
    query2='''select channel_name as channelname,videocount as No_videos from channels order by videocount desc'''
    mycursor.execute(query2)
    mydb.commit
    t2=mycursor.fetchall()
    df2=pd.DataFrame(t2,columns=["channel name","No of videos"])
    st.write(df2)
elif question =="3. Top 10 Most viewed Video":
    query3='''select views as views,channel_name as channelnames,title as videotitle from videos where views is not null order by views desc limit 10'''
    mycursor.execute(query3)
    mydb.commit
    t3=mycursor.fetchall()
    df3=pd.DataFrame(t3,columns=["Views","Channel name","video title"])
    st.write(df3)
elif question =="4. Comments in each video":
    query4='''select comments_count as No_comments,title as videotitle from videos where comments_count is not null'''
    mycursor.execute(query4)
    mydb.commit
    t4=mycursor.fetchall()
    df4=pd.DataFrame(t4,columns=["Comments count","videotitle"])
    st.write(df4)
elif question =="5. Videos with highest likes":
    query5='''select title as videotitle,channel_name as channnelname,likes as likecount from videos where likes is not null order by likes desc'''
    mycursor.execute(query5)
    mydb.commit
    t5=mycursor.fetchall()
    df5=pd.DataFrame(t5,columns=["videotitle","channel_name","likes"])
    st.write(df5)
elif question =="6. Likes of all videos":
    query6='''select likes as likescount,title as videotitle from videos'''
    mycursor.execute(query6)
    mydb.commit
    t6=mycursor.fetchall()
    df6=pd.DataFrame(t6,columns=["likes","videotitle"])
    st.write(df6)
elif question =="7. views of each channels":
    query7='''select channel_name as ChannelName,viewcount as totalviewcount from channels'''
    mycursor.execute(query7)
    mydb.commit
    t7=mycursor.fetchall()
    df7=pd.DataFrame(t7,columns=["channelname","Viewcount"])
    st.write(df7)
elif question =="8. Videos publised in the year of 2022":
    query8='''select title as title,upload_date as VideoRelease,channel_name as channelname from videos where extract(year from upload_date)=2022'''
    mycursor.execute(query8)
    mydb.commit
    t8=mycursor.fetchall()
    df8=pd.DataFrame(t8,columns=["Title","PublishedDate","ChannelName"])
    st.write(df8)
elif question =="9. Average duration of all videos in each channel":
    query9='''select channel_name as channelname,AVG(vid_lenth) as AvarageDuration  from videos group by channel_name'''
    mycursor.execute(query9)
    mydb.commit
    t9=mycursor.fetchall()
    df9=pd.DataFrame(t9,columns=["ChannelName","AvarageDuration"])
    t9=[]
    for index,row in df9.iterrows():
        channel_title=row["ChannelName"]
        average_duration=row["AvarageDuration"]
        average_duration_str=str(average_duration)
        t9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
    df9=pd.DataFrame(t9)
    st.write(df9)
elif question =="10. Videos with highesdt number of comments":
    query10='''select title as VideoTitle,channel_name as ChannelName,comments_count as Comments from videos where comments_count is not null order by comments_count desc'''
    mycursor.execute(query10)
    mydb.commit
    t10=mycursor.fetchall()
    df10=pd.DataFrame(t10,columns=["VideoTitle","ChannelName","Comments"])
    st.write(df10)

