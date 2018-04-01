### COMP90024 Cluster and Cloud Computing
### 2018 Semester 1
### Assignment 1 - HPC Instagram Geoprocessing
### Ivan Ken Weng Chee 🚀
### 736901

import json
import numpy as np
import operator
import sys
import time
from mpi4py import MPI
from pprint import pprint

def loadData():
    for i in range(len(sys.argv)):
        sys.argv[i]
    data = json.load(open('data/melbGrid.json'))
    insta = json.load(open('data/tinyInstagram.json'))
    return data, insta

"""
def getPosts(insta, coords):
    posts = {}
    rows = {}
    cols = {}
    for i in range(len(insta['rows'])):
        if 'coordinates' in insta['rows'][i]['doc']:
            loc = {
                'x': insta['rows'][i]['doc']['coordinates']['coordinates'][1],
                'y': insta['rows'][i]['doc']['coordinates']['coordinates'][0]
            }
            square = inside(loc, coords)
            if square != None:
                if square not in posts:
                    posts[square] = {'count': 0, 'content': []}
                posts[square]['content'].append(insta['rows'][i]['doc'])
                posts[square]['count'] += 1
                if square[0] not in rows:
                    rows[square[0]] = {'count': 0, 'content': []}
                rows[square[0]]['content'].append(insta['rows'][i]['doc'])
                rows[square[0]]['count'] += 1
                if square[1] not in cols:
                    cols[square[1]] = {'count': 0, 'content': []}
                cols[square[1]]['content'].append(insta['rows'][i]['doc'])
                cols[square[1]]['count'] += 1
    return posts, rows, cols
"""
def getPosts(insta, coords):
    posts = {}
    rows = {}
    cols = {}
    for i in range(len(insta)):
        if 'coordinates' in insta[i]['doc']:
            loc = {
                'x': insta[i]['doc']['coordinates']['coordinates'][1],
                'y': insta[i]['doc']['coordinates']['coordinates'][0]
            }
            square = inside(loc, coords)
            if square != None:
                if square not in posts:
                    posts[square] = {'count': 0, 'content': []}
                posts[square]['content'].append(insta[i]['doc'])
                posts[square]['count'] += 1
                if square[0] not in rows:
                    rows[square[0]] = {'count': 0, 'content': []}
                rows[square[0]]['content'].append(insta[i]['doc'])
                rows[square[0]]['count'] += 1
                if square[1] not in cols:
                    cols[square[1]] = {'count': 0, 'content': []}
                cols[square[1]]['content'].append(insta[i]['doc'])
                cols[square[1]]['count'] += 1
    return posts, rows, cols

def getCoordinates(data):
    coords = []
    for i in range(len(data['features'])):
        coords.append({})
        coords[i]['id'] = str(data['features'][i]['properties']['id'])
        coords[i]['xmin'] = data['features'][i]['properties']['xmin']
        coords[i]['xmax'] = data['features'][i]['properties']['xmax']
        coords[i]['ymin'] = data['features'][i]['properties']['ymin']
        coords[i]['ymax'] = data['features'][i]['properties']['ymax']
    return coords

def inside(location, coords):
    for i in range(len(coords)):
        if location['x'] >= coords[i]['xmin'] and location['x'] <= coords[i]['xmax'] and location['y'] >= coords[i]['ymin'] and location['y'] <= coords[i]['ymax']:
            return coords[i]['id']
    return None

def order(posts, rows, cols):
    sortedPosts = sorted(posts.items(), key=operator.itemgetter(1))
    sortedRows = sorted(rows.items(), key=operator.itemgetter(1))
    sortedCols = sorted(cols.items(), key=operator.itemgetter(1))
    print('**** Rank by Unit ******')
    for i in range(len(sortedPosts)):
        print(sortedPosts[i][0] + ': ' + str(sortedPosts[i][1]['count']) + ' posts')
    print('**** Rank by Row *******')
    for i in range(len(sortedRows)):
        print(sortedRows[i][0] + '-Row: ' + str(sortedRows[i][1]['count']) + ' posts')
    print('**** Rank by Column ****')
    for i in range(len(sortedCols)):
        print('Column ' + sortedCols[i][0] + ': ' + str(sortedCols[i][1]['count']) + ' posts')

def merge(x, y):
    for key in y:
        if key in x:
            x[key]['content'] += y[key]['content']
            x[key]['count'] += y[key]['count']
        else:
            x[key] = y[key]
    return x

def main():
    comm = MPI.COMM_WORLD
    size = comm.Get_size() # Number of processes
    rank = comm.Get_rank() # Process number
    name = MPI.Get_processor_name()

    # Master
    if rank == 0:
        start = time.time()
        data, insta = loadData()
        coords = getCoordinates(data)

        # Split data into equal sized chunks
        chunks = np.array_split(insta['rows'], size)

        # Send data to workers
        for i in range(1, size):
            comm.send((coords, chunks[i]), dest=i)

        # Process chunk #0
        posts, rows, cols = getPosts(chunks[0], coords)

        # Receive processed data from workers
        for i in range(1, size):
            (p, r, c) = comm.recv(source=i)
            posts = merge(posts, p)
            rows = merge(rows, r)
            cols = merge(cols, c)
        order(posts, rows, cols)

        end = time.time()
        print('Time: ' + str(end - start) + 's')

    # Worker
    elif rank > 0:
        #print("I am process %d of %d on %s\n" % (rank, size, name))
        (coords, insta) = comm.recv(source=0)

        # Process chunk #rank
        posts, rows, cols = getPosts(insta, coords)
        comm.send((posts, rows, cols), dest=0)

if __name__ == '__main__':
    main()
