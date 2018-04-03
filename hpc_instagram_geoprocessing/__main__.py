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
    sortedPosts = sorted(posts.items(), key=operator.itemgetter(1), reverse=True)
    sortedRows = sorted(rows.items(), key=operator.itemgetter(1), reverse=True)
    sortedCols = sorted(cols.items(), key=operator.itemgetter(1), reverse=True)
    print('**** Rank by Unit ******')
    for i in range(len(sortedPosts)):
        print(sortedPosts[i][0] + ': ' + str(sortedPosts[i][1]) + ' posts')
    print('**** Rank by Row *******')
    for i in range(len(sortedRows)):
        print(sortedRows[i][0] + '-Row: ' + str(sortedRows[i][1]) + ' posts')
    print('**** Rank by Column ****')
    for i in range(len(sortedCols)):
        print('Column ' + sortedCols[i][0] + ': ' + str(sortedCols[i][1]) + ' posts')

def counterSummation(x, y, datatype):
    for key in y:
        if key in x:
            x[key] += y[key]
        else:
            x[key] = y[key]
    return x

def main():
    comm = MPI.COMM_WORLD
    size = comm.Get_size() # Number of processes
    rank = comm.Get_rank() # Process number
    name = MPI.Get_processor_name()

    # Master Node
    if rank == 0:
        start = time.clock()
        posts = {}
        rows = {}
        cols = {}

        # Open coordinates
        with open('data/melbGrid.json', 'r') as file:
            data = json.load(file)
        # Send coordinates to workers
        coords = getCoordinates(data)
        for i in range(1, size):
            comm.send(coords, dest=i)

        # Open posts
        with open('data/tinyInstagram.json', 'r') as file:
            insta = json.load(file)['rows']

        # -n > 1
        if (size > 1):
            turn = 1
            for i in range(len(insta)):
                if 'coordinates' in insta[i]['doc']:
                    loc = {
                        'x': insta[i]['doc']['coordinates']['coordinates'][1],
                        'y': insta[i]['doc']['coordinates']['coordinates'][0]
                    }
                    comm.send(loc, dest=turn)

                # Change rank
                if turn < (size - 1):
                    turn += 1
                else:
                    turn = 1

            # Send terminate signal
            for i in range(1, size):
                comm.send('terminate', dest=i)

            comm.reduce(posts, op=MPI.Op.Create(counterSummation, commute=True))
            comm.reduce(rows, op=MPI.Op.Create(counterSummation, commute=True))
            comm.reduce(cols, op=MPI.Op.Create(counterSummation, commute=True))

        # -n = 1
        else:
            for i in range(len(insta)):
                if 'coordinates' in insta[i]['doc']:
                    loc = {
                        'x': insta[i]['doc']['coordinates']['coordinates'][1],
                        'y': insta[i]['doc']['coordinates']['coordinates'][0]
                    }
                    square = inside(loc, coords)
                    if square != None:
                        if square not in posts:
                            posts[square] = 0
                        posts[square] += 1
                        if square[0] not in rows:
                            rows[square[0]] = 0
                        rows[square[0]] += 1
                        if square[1] not in cols:
                            cols[square[1]] = 0
                        cols[square[1]] += 1

        order(posts, rows, cols)

        end = time.clock()
        print('Time: ' + str(end - start) + 's')

    # Worker Node
    elif rank > 0:
        posts = {}
        rows = {}
        cols = {}
        coords = comm.recv(source=0)

        # Keeps receiving lines from master until terminate signal is received
        while True:
            loc = comm.recv(source=0)
            if loc == 'terminate':
                break
            square = inside(loc, coords)
            if square != None:
                if square not in posts:
                    posts[square] = 0
                posts[square] += 1
                if square[0] not in rows:
                    rows[square[0]] = 0
                rows[square[0]] += 1
                if square[1] not in cols:
                    cols[square[1]] = 0
                cols[square[1]] += 1

        comm.reduce(posts, op=MPI.Op.Create(counterSummation, commute=True))
        comm.reduce(rows, op=MPI.Op.Create(counterSummation, commute=True))
        comm.reduce(cols, op=MPI.Op.Create(counterSummation, commute=True))

if __name__ == '__main__':
    main()
