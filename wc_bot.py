import glob
import os
from sched import scheduler
import time
import requests
import telebot
import requests
import pandas as pd

TOKEN = "6520068688:AAFsISoEMAMhZNVnBkBX09YtOfGel1WhgZQ"
URL = f"https://api.telegram.org/bot{TOKEN}/getUpdates"
PATH_TO_MAPS = "./"
MAP_PATTERN = '*_map_*.html'
PATH_TO_USERS = 'users.pqt'
PIN = "094684"

#users_df = pd.read_parquet(PATH_TO_USERS)  # (columns=["username", "chat_id", "subscribed", "role"])
bot = telebot.TeleBot(TOKEN)

# def get_users():
#     r = requests.get(URL)
#     results = r.json()["result"]    
#     rows = []
#     print(len(results))
#     for item in results:
#         chat = item["message"]["chat"]
#         username = item["message"]["from"]["username"]
#         chat_id = chat["id"]
#         rows.append({chat_id:username})
#         print(item, rows[-1])
#     return rows

#read pqt dataframe

#df = pd.DataFrame(columns=["username", "chat_id", "subscribed", "role"])
# стащить всех юзеров
# users_dict = get_users()
# # update users data
# for u in users_dict:
#     print(u)
#     if not df['chat_id'].isin([u]).any():
#         df = df.append(pd.Series({"username": users_dict[u], "chat_id": u, "subscribed": False, "role": False}), ignore_index=True)
# #df.to_parquet(PATH_TO_USERS)
#df.to_csv(PATH_TO_USERS, index=False)


def write_users(df):
    while (True):
        try:
            df.to_csv(PATH_TO_USERS, index=False)            
            break
        except:
            pass

# Обработчик команды /start
@bot.message_handler(commands=["start"])
def start(message):
    try:
        users_df = pd.read_csv(PATH_TO_USERS)
    except:
        users_df = pd.DataFrame(columns=["username", "chat_id", "subscribed", "role"])

    chat_id = message.chat.id
    user_name = message.chat.username
    if not users_df['chat_id'].isin([chat_id]).any():
        temp_df = pd.DataFrame({"username": [user_name], "chat_id": [chat_id], "subscribed": [False], "role": [False]})
        users_df =  pd.concat([users_df, temp_df], axis=0)
    #print(users_df.head())
    write_users(users_df)
    bot.clear_step_handler_by_chat_id(message.chat.id)
    #bot.register_next_step_handler(message, subscription)
    markup = telebot.types.ReplyKeyboardMarkup(row_width=2, one_time_keyboard=True)
    btn_yes = telebot.types.KeyboardButton("Да")
    btn_no = telebot.types.KeyboardButton("Нет")
    markup.add(btn_yes, btn_no)
    bot.send_message(message.chat.id, "Хотите подписаться на получение карты ПВЗ?", reply_markup=markup)
    write_users(users_df)
    bot.register_next_step_handler(message, handle_start_buttons)


@bot.message_handler(commands=["subscription"])
def subscription(message):
    bot.clear_step_handler_by_chat_id(message.chat.id)
    markup = telebot.types.ReplyKeyboardMarkup(row_width=2, one_time_keyboard=True)
    btn_yes = telebot.types.KeyboardButton("Да")
    btn_no = telebot.types.KeyboardButton("Нет")
    markup.add(btn_yes, btn_no)
    bot.send_message(message.chat.id, "Хотите подписаться на получение карты ПВЗ?", reply_markup=markup)
    bot.register_next_step_handler(message, handle_start_buttons)


def handle_start_buttons(message):
    users_df = pd.read_csv(PATH_TO_USERS)
    chat_id = message.chat.id
    if message.text == "Да":
        users_df.loc[users_df["chat_id"] == chat_id, "subscribed"] = True
        bot.send_message(chat_id, "Вы подписаны на получение карты ПВЗ!", reply_markup=telebot.types.ReplyKeyboardRemove())
    elif message.text == "Нет":
        users_df.loc[users_df["chat_id"] == chat_id, "subscribed"] = False
        bot.send_message(chat_id, "Хорошо, если передумаете - пишите!", reply_markup=telebot.types.ReplyKeyboardRemove())
    write_users(users_df)
    bot.clear_step_handler_by_chat_id(chat_id)


# Обработчик команды /admin
@bot.message_handler(commands=["admin"])
def admin_command(message):
    chat_id = message.chat.id
    bot.send_message(chat_id, "Введите пин-код:")
    bot.register_next_step_handler(message, check_admin_pin)

# Проверка пин-кода админа
def check_admin_pin(message):
    users_df = pd.read_csv(PATH_TO_USERS)
    chat_id = message.chat.id
    if message.text == PIN:
        bot.send_message(chat_id, "Добро пожаловать, админ!")
        index = users_df.index[users_df["chat_id"] == chat_id].tolist()[0]
        users_df.iloc[index, users_df.columns.get_loc("role")] = True
        write_users(users_df)
        # Добавьте здесь необходимые действия администратора
    else:
        bot.send_message(chat_id, "Неверный пин-код. Попробуйте еще раз.")

@bot.message_handler(commands=["map"])
def send_map_to_chat(message):
    chat_id = message.chat.id
    maps = glob.glob(os.path.join(PATH_TO_MAPS, MAP_PATTERN))
# Нахождение самого последнего файла
    latest_file = max(maps, key=os.path.getmtime)
    f = open(latest_file, "rb")
    bot.send_document(chat_id, f)




bot.set_my_commands([
    telebot.types.BotCommand('/start', 'Запустить бота'),
    telebot.types.BotCommand('/subscription', 'Управление своей подпиской'),
    telebot.types.BotCommand('/map', 'Получить последнюю актуальную карту'),    
])


# Запуск бота
bot.polling()