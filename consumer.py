import time
import random
import boto3
import json


def get_driver(drivers):
    return random.randint(random.randint(0, len(drivers) - 1))


def count_regions(region_dict, region):
    region_dict[region] = region_dict[region] + 1


def process_orders_created(total_money, records, region_dict, order_dict):
    """
    Processes records that come in on the orders_created stream for relevant stats
    :param total_money: running total of money in created orders. Int
    :param records: order_created records.
    :param region_dict: dict for counting region occurrences
    :param order_dict: order dict for holding price and creation time. Keyed on orderid
    :return: returns inputs to main scope
    """
    for row in records['Records']:
        data = json.loads(row['Data'])
        from_region = data['ship_from_region']
        count_regions(region_dict, from_region)
        total_money = total_money + data['price']
        order_dict[data['orderid']] = {'time': row['ApproximateArrivalTimestamp']}
        order_dict[data['orderid']]['price'] = data['price']
    return total_money, region_dict, order_dict


def process_assigned_orders(records, order_dict, reponse_time):
    """
    Processes records that come in on the orders_assigned stream for relevant stats
    :param records: order_created records.
    :param response_time: list of runtimes in seconds
    :param order_dict: order dict for holding price and creation time. Keyed on orderid
    :return: returns inputs to main scope
    """
    for row in records['Records']:
        data = json.loads(row['Data'])
        time = row['ApproximateArrivalTimestamp']
        orderid = data['orderid']
        time_diff = (time - order_dict[orderid]['time']).total_seconds()
        reponse_time.append(time_diff)
    return order_dict, reponse_time


def process_completed_orders(records, order_dict, completed_time, completed_money):
    """
    Processes records that come in on the orders_completed stream for relevant stats
    :param completed_money: running total of money in completed orders. Int
    :param records: order_created records.
    :param completed_time: list of runtimes in seconds
    :param order_dict: order dict for holding price and creation time. Keyed on orderid
    :return: returns inputs to main scope
    """
    for row in records['Records']:
        data = json.loads(row['Data'])
        time = row['ApproximateArrivalTimestamp']
        orderid = data['orderid']
        time_diff = (time - order_dict[orderid]['time']).total_seconds()
        completed_money = completed_money + order_dict[orderid]['price']
        completed_time.append(time_diff)
    return order_dict, completed_time, completed_money


def get_initial_records(client, stream):
    """
    Gets the first shard iterator and starts with the oldest data. Will back fill anydata from the stream.
    :param client: kinesis client object
    :param stream: name of the stream
    :return: returns response which holds shard_iterator and oldest records from stream
    """
    response = client.describe_stream(StreamName=stream)

    my_shard_id = response['StreamDescription']['Shards'][0]['ShardId']

    shard_iterator = client.get_shard_iterator(StreamName=stream,
                                               ShardId=my_shard_id,
                                               ShardIteratorType='TRIM_HORIZON')

    my_shard_iterator = shard_iterator['ShardIterator']

    record_response = client.get_records(ShardIterator=my_shard_iterator)

    return record_response


def main():
    regions = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't']
    total_money = 0
    completed_money = 0
    region_dict = dict()
    order_dict = dict()
    response_times = list()
    completion_time = list()
    created_stream = 'order_stream'
    completed_stream = 'completed_stream'
    assigned_stream = 'assigned_stream'
    for r in regions:
        region_dict[r] = 0
    client = boto3.client('kinesis', endpoint_url='http://localhost:7000')

    created_data = get_initial_records(client, created_stream)
    assigned_data = get_initial_records(client, assigned_stream)
    completed_data = get_initial_records(client, completed_stream)

    total_money, region_dict, order_dict = process_orders_created(total_money, created_data, region_dict, order_dict)

    order_dict, response_times = process_assigned_orders(assigned_data, order_dict, response_times)

    order_dict, completion_time, completed_money = process_completed_orders(completed_data, order_dict, completion_time,
                                                                            completed_money)

    while 'NextShardIterator' in created_data:
        created_data = client.get_records(ShardIterator=created_data['NextShardIterator'])
        assigned_data = client.get_records(ShardIterator=assigned_data['NextShardIterator'])
        completed_data = client.get_records(ShardIterator=completed_data['NextShardIterator'])

        total_money, region_dict, order_dict = process_orders_created(total_money, created_data, region_dict,
                                                                      order_dict)
        order_dict, response_times = process_assigned_orders(assigned_data, order_dict, response_times)

        order_dict, completion_time, completed_money = process_completed_orders(completed_data, order_dict,
                                                                                completion_time, completed_money)

        sorted_regions = sorted(region_dict, key=region_dict.get)
        print "The top 10 regions to ship from, in order, are: {}".format(sorted_regions[:10])
        print "Total Money Taken: ${}".format(total_money)
        print "Total Money Completed: ${}".format(completed_money)
        if response_times:
            print "Average response time: {}".format(sum(response_times) / len(response_times))

        if completion_time:
            print "Average completion time: {}".format(sum(completion_time) / len(completion_time))

        # wait for 5 seconds
        time.sleep(1)


if __name__ == '__main__':
    main()
