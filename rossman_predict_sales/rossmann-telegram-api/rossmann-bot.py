import requests
import os
import pandas as pd
import json
import requests
from flask import Flask, request, Response
## importing the load_dotenv from the python-dotenv module
from dotenv import load_dotenv

load_dotenv()

# TOKEN BOT
#TOKEN = constants.TOKEN
TOKEN = os.getenv("TOKEN")

# info Bot
#https://api.telegram.org/bot{TOKEN}/getMe

# Envia mensagem para o telegram
#https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id=726967414&text=Hi%20Humberto!

#webHook https://c1d04b5379606f.localhost.run
#https://api.telegram.org/bot{TOKEN}/setWebhook?url=https://rossmann-telegram-bot14.herokuapp.com

def sendMessage(chat_id, text):

    url = 'https://api.telegram.org/bot{}'.format(TOKEN)
    url = url + '/sendMessage?chat_id={}'.format(chat_id)

    r = requests.post(url, json={'text': text})
    print('Status code {}'.format(r.status_code))

    return None


def load_dataset(store_id):

    df11 = pd.read_csv('test.csv', low_memory=False)
    df_store_raw = pd.read_csv('store.csv', low_memory=False)

    # merge
    df_test = pd.merge(df11, df_store_raw, how='left', on='Store')

    # choose store
    df_test = df_test.loc[df_test['Store'] == store_id]

    if not df_test.empty:
        # remove close days
        df_test = df_test[df_test['Open'] != 0]
        df_test = df_test[~df_test['Open'].isnull()]
        df_test = df_test.drop('Id', axis=1)

        # convert to json
        data = json.dumps(df_test.to_dict(orient='records'))
    else:
        data = 'error'

    return data


def predict(data):
    # API Call
    url = 'https://rossmann-model-test-14.herokuapp.com/rossmann/predict'
    header = {'Content-type': 'application/json'}
    data = data

    r = requests.post(url, data=data, headers=header)
    print('Status code: {}'.format(r.status_code))

    df1 = pd.DataFrame(r.json(), columns=r.json()[0].keys())

    return df1


def parse_message(message):

    chat_id = message['message']['chat']['id']
    store_id = message['message']['text']

    store_id = store_id.replace('/', '')

    try:
        store_id = int(store_id)
    except ValueError:
        store_id = 'error'

    return chat_id, store_id


# App Inicializa
app = Flask(__name__)


@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        message = request.get_json()

        chat_id, store_id = parse_message(message)

        if store_id != 'error':
            # loading data
            data = load_dataset(store_id)
            if data != 'error':
                # prediction
                df1 = predict(data)

                # calculation
                df2 = df1[['store', 'prediction']].groupby(
                    'store').sum().reset_index()
                msg = 'Store Number {} will sell  R$ {:,.2f} in the next 6 weeks'.format(
                        df2['store'].values[0],
                        df2['prediction'].values[0])

                # send message
                sendMessage(chat_id, msg)
                return Response('Ok', status=200)
            else:
                sendMessage(chat_id, " Store not available")
                return Response('OK', status=200)

        else:
            sendMessage(chat_id, 'Store Id is wrong')
            return Response('OK', status=200)
    else:
        return '<h1>Rossmann Telegram Bot</h1>'


if __name__ == '__main__':
    port = os.environ.get('PORT', 5000)
    app.run('0.0.0.0', port=port)

