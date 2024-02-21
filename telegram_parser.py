#!/usr/bin/env python
# coding: utf-8


import asyncio
from datetime import date, datetime, timedelta
from telethon import TelegramClient
from telethon.tl import functions, types
import json
from telethon.tl.types import InputMessagesFilterEmpty

import private_information

dict_event=private_information.dict_event 
#dict_event=[{'event_name': 'Buki', 'description':'https://t.me/speaking_club_easyschool'}]

chanel_list_all=['https://t.me/smalltalksproject',  'https://t.me/speaking_club_nice', 'https://t.me/your_english_bro']

def telegram_function(events_name):
    api_id = int(private_information.api_id)
    api_hash = private_information.api_hash
    username = private_information.username
    save_file_path=private_information.save_file_path

    today =datetime.today()
    seven_days_befor_today = today - timedelta(days = 7)
    #seven_days_befor_today=date(2023, 9, 1)
    all_messages=[]

    client = TelegramClient(username, api_id, api_hash)
    client.start()

    async def client_telegram(chanel_list):
        for chanel_name in chanel_list:
            chanel_messages=[]
            try:  

                '''
                for some reason the arguments reverse = True and search='str'  don't work together:
                https://docs.telethon.dev/en/stable/modules/client.html#telethon.client.messages.MessageMethods.iter_messages
                so if we want to search for a match by text, it is necessary to exclude the search from a date
                '''

                if chanel_name=='https://t.me/your_english_bro':  #search for a match by text with limit
                    messages_serch='Speaking Club'
                    reverse = False 
                    offset_date = None  
                    limit=5
                else:  #expected that all data is after a date (seven_days_befor_today)
                    #defult
                    messages_serch=None 
                    reverse=True
                    offset_date = seven_days_befor_today
                    limit=30

                channel = await client.get_entity(chanel_name)
                #messages = await client.get_messages(channel,  limit=15, reverse = True, offset_date = seven_days_befor_today, search='Speaking Club') #pass your own args
                messages = await client.get_messages(channel,  limit=limit,  reverse = reverse, offset_date = offset_date, search=messages_serch) 
                #then if you want to get all the messages text
                for x in messages:
                    text_messages=x.text #return text of message
                    date_messages=x.date #return date

                    dict_message= {"date": date_messages, "description": chanel_name, "message": text_messages}
                    chanel_messages.append(dict_message)


                if len(chanel_messages)==0:
                    print('\nDon\'t have any new message for chanel %s ' % ( chanel_name))
                else:
                    print('\nWe get %s new messages for chanel %s ' % (len(chanel_messages), chanel_name))
                    chanel_messages_date = [x['date'].date() for x in chanel_messages]
                    print('Min date:', min(chanel_messages_date), '\tMax date:', max(chanel_messages_date))


                all_messages.extend(chanel_messages)
            except ValueError:
                print('We couldn\'t get data from the site %s ' % ( chanel_name))

        total_site=len(set([d['description'] for d in all_messages if 'description' in d]))        
        with open(save_file_path, 'w') as f:
            #f.write(str(chanel_messages))
            json.dump(all_messages, f, indent=4, sort_keys=True, default=str)
        print('\nToatl: We get %s new messages since %s for  %s  chanel' % (len(all_messages), (seven_days_befor_today).date(), total_site))
        await client.disconnect()  #close client becouse after we be call client again



    
    if events_name=='':
        chanel_list=chanel_list_all
        loop = asyncio.get_event_loop()
        loop.run_until_complete(client_telegram(chanel_list))
        successful_save_path=private_information.save_file_path
        #write all inforamtion
        print('Finish for all chanel and save in', successful_save_path)

        return successful_save_path
    else:
        my_chanel=[i['description'] for i in dict_event if i['event_name'] == events_name][0]
        if  my_chanel  in chanel_list_all:
            chanel_list=[my_chanel]
            loop = asyncio.get_event_loop()
            loop.run_until_complete(client_telegram(chanel_list))
            successful_save_path=private_information.save_file_path
            #write all inforamtion
            print('Finish for one chanel and save in', successful_save_path)

            return successful_save_path
        else:
            print('We pass function. Chanel not use telegram')



#events_name='Small Talks'    
telegram_function('')

