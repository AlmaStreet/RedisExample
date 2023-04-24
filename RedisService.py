import redis
import random


class RedisHelper:
    r = None
    replicas = []
    host = 'localhost'
    last_port = None
    start = None
    end = None
    key_replica_limit = 5

    # shard_limit = 5

    def __init__(self):
        # create redis client
        port = 6379
        self.r = redis.Redis(host=self.host, port=port)
        self.r.config_set('bind', '0.0.0.0')
        self.r.config_set('protected-mode', 'no')
        # self.r.config_set('requirepass', 'your_password')
        self.last_port = port

    def set(self, key, value):
        start = self.r.time()
        self.r.set(key, value)
        end = self.r.time()

        # create key replicas within existing redis server
        for item in range(self.key_replica_limit):
            self.r.set(key + f'.copy_{item}', value)

        # store a replica as shards
        for shard_index, char in enumerate(str(value)):
            self.r.set(key + f'.shard_{shard_index}', char)

        # set values in replicas
        for r in self.replicas:
            r.set(key, value)

        return f'set [{key}:{value}] in {self.perf_test(start, end)}'

    def get(self, key):
        print(self.r.get(key))
        random_copy = int(random.random() * self.key_replica_limit)
        print(f'Printing key replica: copy_{random_copy}')
        print(self.r.get(key + f'.copy_{random_copy}'))

        failed_copies = set([random_copy])

        print('Simulating fetch failure, retrieving data from key replicas')
        for copy in range(self.key_replica_limit):
            failure_rate = random.random()
            if copy not in failed_copies:
                fail = failure_rate > 0.3
                print(f'failure rate: {failure_rate}')

                if fail:
                    print(f'Fetch failed: {key + f".copy_{copy}"}')
                    print('Retrying fetch ...')
                    failed_copies.add(copy)
                else:
                    print(f'{key}.copy_{copy}: {self.r.get(key + f".copy_{copy}")}')
                    break

        if len(failed_copies) >= self.key_replica_limit:
            print('Failed to fetch any keys')

        # reconstructing data from shard
        shard_index = 0
        combined_shard = b''
        while shard_value := self.r.get(key + f'.shard_{shard_index}'):
            print(key + f'.shard_{shard_index}: {shard_value}')
            combined_shard += shard_value
            shard_index += 1

        print(f'combined_shard: {combined_shard}')

    def expire(self, name, time):
        self.r.pexpire(name, time)

    def lpush(self, ls, value):
        self.r.lpush(ls, value)

        for item in range(self.key_replica_limit):
            self.r.lpush(ls + f'.copy_{item}', value)

        for r in self.replicas:
            r.lpush(ls, value)

    def rpush(self, ls, value):
        self.r.rpush(ls, value)

        for item in range(self.key_replica_limit):
            self.r.rpush(ls + f'.copy_{item}', value)

        for r in self.replicas:
            r.rpush(ls, value)

    def lpop(self, ls, count):
        print(self.r.lpop(ls, count))
        print(self.r.lpop(ls + f'.copy_{int(random.random() * self.key_replica_limit)}', count))

    def rpop(self, ls, count):
        print(self.r.rpop(ls, count))
        print(self.r.rpop(ls + f'.copy_{int(random.random() * self.key_replica_limit)}', count))

    def print_d(self):
        for key in redis_helper.r.scan_iter('*'):
            print(key)

    def print_ls(self, ls):
        return self.r.lrange(ls, 0, -1)

    def perf_test(self, start, end):
        sec = end[0] - start[0]
        mill = end[1] - start[1]

        total_time = sec * 1000000 + mill
        new_sec = total_time // 1000000
        new_mill = total_time % 1000000

        return f'{new_sec}.{new_mill} seconds'

    def serialize_set(self):
        return

    def pipeline(self, times=1000000, toggle=True):

        start = self.r.time()
        if toggle:
            pipeline = self.r.pipeline()
            for item in range(times):
                pipeline.ping()
            pipeline.execute()
        else:
            for item in range(times):
                self.r.ping()
        end = self.r.time()

        print('===== ===== ===== ===== =====')
        print(f'Ran Ping Command {times} times {"not " if not toggle else ""}using pipelines')
        print(self.perf_test(start, end))

        return

    # ===== Output =====
    # redis_helper.pipeline()
    # ===== ===== ===== ===== =====
    # Ran Ping Command 1000000 times using pipelines
    # 5.451892 seconds
    # redis_helper.pipeline(toggle=False)
    # ===== ===== ===== ===== =====
    # Ran Ping Command 1000000 times not using pipelines
    # 29.439950 seconds

    # def update_replica(self):
    #     for r in self.replicas:
    # repeat same method

    # def create_replica(self):
    #     # need to fix
    #     self.last_port += 1
    #     replica = redis.Redis(host=self.host, port=self.last_port)
    #     # Configure the replica to connect to the master
    #     # replica.config_set('replicaof', self.host, 6379)
    #     replica.config_set('replicaof', 'localhost', 6379)
    #     replica.bgsave()
    #     self.replicas.append(replica)


# Use Redis client to interact with Redis Server
redis_helper = RedisHelper()
redis_helper.set('first_key', 'first_value')
redis_helper.get('first_key')

redis_helper.set('first_num', 123456)
redis_helper.get('first_num')

# redis_helper.create_replica()
