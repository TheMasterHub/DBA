from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import streamlit as st
import pandas as pd
from pymongo import MongoClient
import psycopg2
import isodate

client = MongoClient("mongodb+srv://mongouser:ONxan7Mej0IpSkLS@cluster0.mycuaha.mongodb.net/?retryWrites=true&w=majority")
db = client["YTData"]
Channels_coll = db['Channels']
youtube = build("youtube", "v3", developerKey="AIzaSyCwnM5Xl_jEHednI-N16xmuO9uUxysQmcE")

def get_channel_data(channel_id):
    ch_data=[]
    response = youtube.channels().list(part="snippet,contentDetails,statistics,status", id=channel_id).execute()
    for item in response["items"]:
        data={'Channel_Name': item ["snippet"]["title"],
              'Channel_Id': item["id"],
              'Subscription_Count': item["statistics"]["subscriberCount"],
              'Channel_Views': item["statistics"]["viewCount"],
              'channel_Description': item ["snippet"]["description"],
              'Channel_Type': item["kind"],
              'Channel_Status': item["status"]["privacyStatus"]
              }
        ch_data.append(data)
    return ch_data

def get_playlists(channel_id):
    playlist_data=[]
    response = youtube.playlists().list(part="snippet", channelId=channel_id,maxResults=5).execute()
    for item in response["items"]:
        data={'Playlist_ID': item["id"],'Channel_Id': item["snippet"]["channelId"],'Playlist_Name': item ["snippet"]["title"] }
        playlist_data.append(data)
    return playlist_data

def get_playlist_video_ids(plsid):
    playlist_video_ids = []
    
    # Get the playlists for the channel
    playlists_response = youtube.playlists().list(part="id", id=",".join(plsid),maxResults=5).execute()
    playlist_ids = [item["id"] for item in playlists_response["items"]]

    # Get the video IDs from each playlist
    for playlist_id in playlist_ids:
        playlist_items_response = youtube.playlistItems().list(part="snippet,contentDetails", playlistId=playlist_id,maxResults=5).execute()
        for item in playlist_items_response["items"]:
            data = {'Playlist_ID': item ["snippet"].get("playlistId"),'Video_ID': item["contentDetails"]["videoId"]}
            playlist_video_ids.append(data)
    return playlist_video_ids

def get_videos(vdlst):
    videos = []
    video_details = youtube.videos().list(part="snippet,statistics,contentDetails",id=",".join(vdlst),maxResults=5).execute()
    for item in video_details["items"]:
            data = {
                'Video_ID': item["id"],
                'Video_Name': item["snippet"]["title"],
                'Video_Description': item["snippet"]["description"],
                'Video_PublishedAT': item["snippet"]["publishedAt"],
                'video_tags': item["snippet"].get("tags", [""]),
                'video_view_count': item["statistics"]["viewCount"],
                'video_like_count': item["statistics"]["likeCount"],
                'video_dislike_count': item["statistics"].get("dislikeCount", 0),
                'video_favourite_count': item["statistics"].get("favoriteCount", 0),
                'video_comment_count': item["statistics"].get("commentCount", 0),
                'video_duration': item["contentDetails"]["duration"],
                'video_Caption_Status': item["contentDetails"].get("caption", 0),
                'video_thumbnail': item["snippet"]["thumbnails"].get('default', {}).get('url', 'Thumbnail Not Available')
            }
            videos.append(data)
    return videos


def dbms_mig_channel(selected_channel):
    documents = Channels_coll.find({"Channel.Channel_Name": {"$in": selected_channel}})
    pg_conn = psycopg2.connect(host='localhost',port='5432',database='postgres',user='postgres',password='postgres')
    pg_cursor = pg_conn.cursor()
    for document in documents:
        Channel = document['Channel']
        for ch in Channel:
            Channel_ID = ch['Channel_Id']
            Channel_Name = ch['Channel_Name']
            Channel_Type = ch['Channel_Type']
            Channel_Views = ch['Channel_Views']
            Channel_description = ch['channel_Description']
            Channel_Status = ch['Channel_Status']
            insert_channel = '''INSERT INTO "ganesh"."channel" ("channel_id","channel_name","channel_type","channel_views","channel_description","channel_status") VALUES (%s, %s, %s, %s, %s, %s);'''
            pg_cursor.execute(insert_channel, (Channel_ID, Channel_Name,Channel_Type,Channel_Views,Channel_description,Channel_Status))
        pg_conn.commit()
    pg_cursor.close()

def dbms_mig_playlist(selected_channel):
    documents1 = Channels_coll.find({"Channel.Channel_Name": {"$in": selected_channel}})
    pg_conn1 = psycopg2.connect(host='localhost',port='5432',database='postgres',user='postgres',password='postgres')
    pg_cursor1 = pg_conn1.cursor()
    for document1 in documents1:
        Playlist_Items = document1['Playlist_Items']
        for playlist_item in Playlist_Items:
            value1 = playlist_item['Playlist_ID']
            value2 = playlist_item['Channel_Id']
            value3 = playlist_item['Playlist_Name']
            insert_query = """ INSERT INTO "ganesh"."playlist" ("playlist_id", "channel_id", "playlist_name") VALUES (%s, %s, %s);"""
            pg_cursor1.execute(insert_query, (value1, value2, value3))
        pg_conn1.commit()
    pg_cursor1.close()

def dbms_mig_video(selected_channel):
    documents2 = Channels_coll.find({"Channel.Channel_Name": {"$in": selected_channel}})
    pg_conn2 = psycopg2.connect(host='localhost',port='5432',database='postgres',user='postgres',password='postgres')
    pg_cursor2 = pg_conn2.cursor()    
    for document2 in documents2:
        Vid_Items = document2['Videos_List']
        for vid_item in Vid_Items:
            value1 = vid_item['Video_ID']
            value22 = vid_item['Playlist_ID']
            value2 = vid_item['Video_Name']
            value3 = vid_item['Video_Description']
            value4 = vid_item['Video_PublishedAT']
            value5 = vid_item['video_view_count']
            value6 = vid_item['video_like_count']
            value7 = vid_item['video_dislike_count']
            value8 = vid_item['video_favourite_count']
            value9 = vid_item['video_comment_count']
            value110 = vid_item['video_duration']
            value120 = isodate.parse_duration(value110)
            value10 = (int(value120.total_seconds()))
            value11 = vid_item['video_thumbnail']
            value12 = vid_item['video_Caption_Status']
            insert_query = """ INSERT INTO "ganesh"."video" ("video_id","playlist_id", "video_name", "video_description", "published_date", "view_count", "like_count",
                            "dislike_count", "favourite_count", "comment_count", "duration", "thumbnail", "caption_status")
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
            pg_cursor2.execute(insert_query, (value1, value22, value2, value3,value4,value5,value6,value7,value8,value9,value10,value11,value12))
        pg_conn2.commit()
    pg_cursor2.close()
 
def select():
    pg_conn3 = psycopg2.connect(host='localhost',port='5432',database='postgres',user='postgres',password='postgres')
    pg_cursor3 = pg_conn3.cursor() 
    q1 = """select v.video_name,c.channel_name from ganesh.video as v join ganesh.playlist as p on p.playlist_id=v.playlist_id join ganesh.channel as c on c.channel_id=p.channel_id ;"""
    pg_cursor3.execute(q1)
    rows1 = pg_cursor3.fetchall()
    columns1 = [desc[0] for desc in pg_cursor3.description]
    dftab1 = pd.DataFrame(rows1, columns=columns1)
    st.write("Video Names with Channel Name")
    st.dataframe(dftab1)
    q2 = """SELECT c.channel_name, COUNT(v.video_id) AS video_count FROM ganesh.channel AS c JOIN ganesh.playlist AS p ON c.channel_id = p.channel_id
        JOIN ganesh.video AS v ON p.playlist_id = v.playlist_id GROUP BY c.channel_id, c.channel_name ORDER BY video_count DESC LIMIT 1;"""
    pg_cursor3.execute(q2)
    rows2 = pg_cursor3.fetchall()
    columns2 = [desc[0] for desc in pg_cursor3.description]
    dftab2 = pd.DataFrame(rows2, columns=columns2)
    st.write("Channel with Maximum Number of Videos")
    st.dataframe(dftab2)
    q3 = """ SELECT c.channel_name,v.video_name, v.view_count FROM ganesh.channel AS c JOIN ganesh.playlist AS p ON c.channel_id = p.channel_id
             JOIN ganesh.video AS v ON p.playlist_id = v.playlist_id  ORDER BY view_count DESC LIMIT 10; """
    pg_cursor3.execute(q3)
    rows3 = pg_cursor3.fetchall()
    columns3 = [desc[0] for desc in pg_cursor3.description]
    dftab3 = pd.DataFrame(rows3, columns=columns3)
    st.write("Top 10 videos views with ChannelName")
    st.dataframe(dftab3)
    q4 = """ select video_name,comment_count from ganesh.video; """
    pg_cursor3.execute(q4)
    rows4 = pg_cursor3.fetchall()
    columns4 = [desc[0] for desc in pg_cursor3.description]
    dftab4 = pd.DataFrame(rows4, columns=columns4)
    st.write("Video Name with total number of comment")
    st.dataframe(dftab4)
    q5 = """ SELECT c.channel_name,v.video_name, v.like_count FROM ganesh.channel AS c JOIN ganesh.playlist AS p ON c.channel_id = p.channel_id
         JOIN ganesh.video AS v ON p.playlist_id = v.playlist_id  ORDER BY like_count DESC LIMIT 1; """
    pg_cursor3.execute(q5)
    rows5 = pg_cursor3.fetchall()
    columns5 = [desc[0] for desc in pg_cursor3.description]
    dftab5 = pd.DataFrame(rows5, columns=columns5)
    st.write("Video with Highest Likes count")
    st.dataframe(dftab5)
    q6 = """ select video_name,like_count,dislike_count from ganesh.video order by like_count desc; """
    pg_cursor3.execute(q6)
    rows6 = pg_cursor3.fetchall()
    columns6 = [desc[0] for desc in pg_cursor3.description]
    dftab6 = pd.DataFrame(rows6, columns=columns6)
    st.write("Video with Likes and Dislikes count")
    st.dataframe(dftab6)
    q7 = """ select channel_name,channel_views from ganesh.channel order by channel_views desc; """
    pg_cursor3.execute(q7)
    rows7 = pg_cursor3.fetchall()
    columns7 = [desc[0] for desc in pg_cursor3.description]
    dftab7 = pd.DataFrame(rows7, columns=columns7)
    st.write("Total number of views for each channel")
    st.dataframe(dftab7)
    q8 = """ SELECT distinct c.channel_name FROM ganesh.channel AS c JOIN ganesh.playlist AS p ON c.channel_id = p.channel_id
         JOIN ganesh.video AS v ON p.playlist_id = v.playlist_id where EXTRACT(YEAR FROM v.published_date) = 2022; """
    pg_cursor3.execute(q8)
    rows8 = pg_cursor3.fetchall()
    columns8 = [desc[0] for desc in pg_cursor3.description]
    dftab8 = pd.DataFrame(rows8, columns=columns8)
    st.write("Channels published videos at year 2022")
    st.dataframe(dftab8)
    q9 = """ SELECT c.channel_name, avg(v.duration) AS Average_Video_Duration FROM ganesh.channel AS c JOIN ganesh.playlist AS p ON c.channel_id = p.channel_id
        JOIN ganesh.video AS v ON p.playlist_id = v.playlist_id GROUP BY c.channel_id, c.channel_name ; """
    pg_cursor3.execute(q9)
    rows9 = pg_cursor3.fetchall()
    columns9 = [desc[0] for desc in pg_cursor3.description]
    dftab9 = pd.DataFrame(rows9, columns=columns9)
    st.write("Video with Likes and Dislikes count")
    st.dataframe(dftab9)
    q10 = """ SELECT c.channel_name,v.video_name, v.comment_count FROM ganesh.channel AS c JOIN ganesh.playlist AS p ON c.channel_id = p.channel_id
         JOIN ganesh.video AS v ON p.playlist_id = v.playlist_id  ORDER BY comment_count DESC LIMIT 1;"""
    pg_cursor3.execute(q10)
    rows10 = pg_cursor3.fetchall()
    columns10 = [desc[0] for desc in pg_cursor3.description]
    dftab10 = pd.DataFrame(rows10, columns=columns10)
    st.write("Video with highest Comments count")
    st.dataframe(dftab10)
    pg_cursor3.close()
    

def main():
    st.title("Data Extraction from Youtube")

    # Input channel ID
    channel_id = st.text_input("Enter YouTube Channel ID")
    if st.button("MongoDB Migration"):
        plst = get_playlists(channel_id)
        plst_df = pd.DataFrame(plst)
        plsid = plst_df.iloc[:,0]
        pv_ids = get_playlist_video_ids(plsid)
        pv_ids_df = pd.DataFrame(pv_ids)
        vdlst = pv_ids_df.iloc[:,1]
        vidinfo = get_videos(vdlst)
        vidinfo_df = pd.DataFrame(vidinfo)
        add_plist = pv_ids_df.merge(vidinfo_df, on= "Video_ID" )
        vid_details = add_plist.drop_duplicates(subset='Video_ID', keep="first")
        vid_details_dict = vid_details.to_dict(orient='records')
        channel_data = {"Channel":get_channel_data(channel_id),'Playlist_Items': get_playlists(channel_id),"Videos_List": vid_details_dict}
        Channels_coll.insert_one(channel_data)
        
    
    ch = Channels_coll.find({}, { "Channel": { "Channel_Name": 1 } })
    chdf = pd.DataFrame(ch)
    channel_list = chdf['Channel'].apply(lambda x: x[0]['Channel_Name']).tolist()
    selected_channel = st.multiselect('Select Channel:', channel_list)
    if len(selected_channel) > 0:
        submit_button = st.button('PostgresDB Migration')
        if submit_button:
            dbms_mig_channel(selected_channel)
            dbms_mig_playlist(selected_channel)
            dbms_mig_video(selected_channel)
    select()

                  
if __name__ == '__main__':
    main()



        
