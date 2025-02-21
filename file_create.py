#!/usr/bin/python3
import random

f = open('./data.txt', 'w')
for i in range(3000):
    str = '{};{};{}'.format(random.randint(1000, 100000), random.randint(1000, 100000), random.randint(1000, 100000))
    f.write('{}\n'.format(str))  
f.close()