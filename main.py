import sys

import psycopg
import json
import select
import selectors
import logging
import requests

BASE_URL = 'http://34.241.87.71:8081/'
POST_API_KEY = 'AC8tQF4YAqgne8G90PVlWKxUv48veTmpsYOyHUfMpQDRXlkhlJ9Alsp7nzIKd5Dghumy7fTFheCAVggc3MqFbo90h31Uv81bn6XgxLzCMh70lIuXoiRs591HR1ynrKKj'

logging.basicConfig(level=logging.DEBUG, filename="message.log", filemode="a+",
                    format="%(asctime)-15s %(levelname)-8s %(message)s")


def db_listen():
    logging.basicConfig(level=logging.DEBUG, filename="message.log", filemode="a+",
                        format="%(asctime)-15s %(levelname)-8s %(message)s")

    conn = psycopg.connect('postgresql://augeias:augeias@83.212.19.17:5432/augeias', autocommit=True)

    conn.execute("LISTEN accuweather_mape_changed")
    conn.execute("LISTEN openweather_mape_changed")

    gen = conn.notifies()
    for notify in gen:
        if not notify.payload:
            break
        payload = json.loads(notify.payload)
        if notify.channel == 'openweather_mape_changed':
            table_name = 'OpenWeatherMape'
        else:
            table_name = 'AccuWeatherMape'
        pass
        for key, value in payload.items():
            if not value:
                break
            if key == 'record':
                logging.info(json.dumps(value))

                url = f'{BASE_URL}{table_name}'

                data = json.loads(json.dumps(value))
                try:
                    r = requests.post(headers={'apikey': POST_API_KEY},
                                      url=url, json=data)
                    logging.info(r.status_code)
                    r.raise_for_status()
                except Exception as e:
                    logging.error(e)
                    pass
        if notify.payload == "stop":
            gen.close()


if __name__ == '__main__':
    db_listen()
