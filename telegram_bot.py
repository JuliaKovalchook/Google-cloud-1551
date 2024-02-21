#!/usr/bin/env python
# coding: utf-8

import os
from google.cloud import bigquery
import pandas as pd
import telebot

import private_information #files with non-public credential 


# -----------------------------------------------------------------------------------

#get data from bigquery

my_SERVICE_ACCOUNT_JSON = private_information.SERVICE_ACCOUNT_JSON

client = bigquery.Client.from_service_account_json(my_SERVICE_ACCOUNT_JSON)

def get_data(search_text):
    sql = '''SELECT 
    FORMAT_DATE("%d.%m.%Y",created_at) as date,
    address, title
    FROM `data-eng-408109.db_1551.tb_1551` 
    WHERE address like '%{}%' 
    LIMIT 10'''.format(search_text)
    df = client.query(sql).to_dataframe()
    return df


# -----------------------------------------------------------------------------------
# telegram bot

my_BOT_TOKEN = private_information.BOT_TOKEN

bot = telebot.TeleBot(my_BOT_TOKEN)

name = '';
surname = '';
age = 0;
@bot.message_handler(content_types=['text'])
def start(message):
    if message.text == '/start':
        bot.send_message(message.from_user.id, "Яка вулиця тебе цікавить?");
        bot.register_next_step_handler(message, get_street); 
        
        print('LOG:', datetime.now() , message.text)
    else:
        bot.send_message(message.from_user.id, 'Напиши /start');

def get_street(message): 
    global search_text
    search_text = message.text
    
    bot.send_message(message.from_user.id, 'Шукаємо таке звернення...')
    df=get_data(search_text)
    if len(df)>=1:
        bot.send_message(message.from_user.id, 'Знайшли')
        bot.send_message(message.from_user.id,  '\n\n'.join(str(element) \
                            for element in [row for row in df.values]) )

    else: bot.send_message(message.from_user.id, 'Такого звернення немає')
    bot.send_message(message.from_user.id, 'Для повторну натисни /start')
    #bot.send_message(message.from_user.id, text=df)

bot.infinity_polling()
