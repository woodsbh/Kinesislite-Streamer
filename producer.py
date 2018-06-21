import boto3
import json
import random
import time


def delete_stream(client, name):
    try:
        client.delete_stream(StreamName=name)
    except:
        pass
    time.sleep(1)


def create_stream(client, name, shard):
    client.create_stream(StreamName=name, ShardCount=shard)
    time.sleep(1)


def order_created(client, orderid, stream, regions):
    counter = 0
    order_ids = []
    while counter <= 10:
        counter += 1
        payload = {
            'orderid': orderid,
            'ship_from_region': regions[random.randint(0, len(regions) - 1)],
            'ship_to_region': regions[random.randint(0, len(regions) - 1)],
            'pick up time': '{}:{}'.format(random.randint(1, 25), random.randint(1, 61)),
            'price': random.randint(0, 101)
        }
        client.put_record(StreamName=stream, Data=json.dumps(payload), PartitionKey='aa-bb')
        print payload
        orderid += 1
        order_ids.append(orderid)
        time.sleep(.05)
    return order_ids


def order_assigned(client, orders, drivers, stream):
    counter = 0
    while counter <= random.randint(1, 7):
        open_orders = [x for x in orders.keys() if orders[x] == 0]
        try:
            orderid = random.choice(open_orders)
        except IndexError as e:
            return
        else:
            counter += 1
            payload = {
                'orderid': orderid,
                'driver': random.choice(drivers)
            }
            response = client.put_record(StreamName=stream, Data=json.dumps(payload), PartitionKey='aa-web')
            print payload
            orders[orderid] = 1


def order_completed(client, orders, stream):
    counter = 0
    while counter <= random.randint(1, 3):
        assigned_orders = [x for x in orders.keys() if orders[x] == 1]
        try:
            orderid = random.choice(assigned_orders)
        except IndexError as e:
            return
        else:
            counter += 1
            payload = {
                'orderid': orderid,
            }
            client.put_record(StreamName=stream, Data=json.dumps(payload), PartitionKey='ab')
            print payload
            orders.pop(orderid)


def main():
    regions = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't']
    driver_ids = range(1, 10001)
    orders = {}
    created_stream = 'order_stream'
    completed_stream = 'completed_stream'
    assigned_stream = 'assigned_stream'
    streams = [created_stream, completed_stream, assigned_stream]
    client = boto3.client('kinesis', endpoint_url='http://localhost:7000')

    for x, stream in enumerate(streams):
        delete_stream(client, stream)
        create_stream(client, stream, 2)

    orderid = 0
    while True:
        order_ids = order_created(client, orderid, created_stream, regions)
        orderid = max(order_ids)
        for id in order_ids:
            orders[id] = 0
        if random.randint(1, 10) % 2 == 0:
            order_assigned(client, orders, driver_ids, assigned_stream)
        if random.randint(1, 100) % 10 == 0:
            order_completed(client, orders, completed_stream)


if __name__ == '__main__':
    main()
