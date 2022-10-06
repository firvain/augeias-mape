import sys

import psycopg
import json
import select
import selectors
import logging

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

        payload = json.loads(notify.payload)
        for key, value in payload.items():
            if key == 'record':
                logging.info(json.dumps(value))
        if notify.payload == "stop":
            gen.close()



if __name__ == '__main__':
    db_listen()
