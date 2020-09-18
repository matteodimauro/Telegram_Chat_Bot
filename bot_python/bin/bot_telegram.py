import telepot
from pprint import pprint
import socket
import json

TOKEN = '1347881435:AAGYNteynaOJRguTKTfWyat-ykI4N6vYbxk'

def send_to_logstash(host, port, data):
    error = True
    while(error):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            print("Socket created. sock: " + str(sock))
        
            sock.connect((host, port))
            print("Socket connected to HOST: " + host + " PORT: " + str(port))
            print("Socket connected. Sock: " + str(sock))

            print("Sending message: " + str(data))

            sock.sendall(json.dumps(data).encode())
            print("End connection")
            sock.close()
            error = False
        except:
            print("Connection error. There will be a new attempt in 10 seconds")
            time.sleep(10)

def conti(*args):
    return sum(args), sum(args)/len(args)


def on_chat_message(msg):
    content_type, chat_type, chat_id = telepot.glance(msg)
    response=bot.getUpdates()#crea un json
    if content_type == 'text':
        name = msg["from"]["first_name"]
        txt = msg['text']
        pprint(msg)
        response.append(msg)#aggiunge il messaggio al json
        if txt.startswith('/start'):
            bot.sendMessage(chat_id, 'ciao {}, sono un bot molto stupido!'.format(name))
        
        elif txt.startswith('/help'):
            bot.sendMessage(chat_id, 'Ecco i comandi che capisco:\n - /start\n - /hey\n - /conti')
        elif txt.startswith('/conti'):
            params = txt.split()[1:]
            if len(params) == 0:
                bot.sendMessage(chat_id, 'Uso: /conti <parametri>.. Calcola la somma e la media dei numeri')
            else:
                try:
                    params = [float(param) for param in params]
                    somma, media = conti(*params)
                    bot.sendMessage(chat_id, 'Somma: {}, media {}'.format(somma, media))
                except:
                    bot.sendMessage(chat_id, 'Errore nei parametri, non hai inserito numeri!')
        send_to_logstash('10.0.100.8', 5000, response)

if __name__ == '__main__':
    bot = telepot.Bot(TOKEN)
    response=bot.getUpdates()
    bot.sendMessage(-432449020, 'bot started')
    pprint(response)
    bot.message_loop(on_chat_message)
    

    print('Listening ...')

import time
while 1:
    time.sleep(10)