'''
File    : hpc_instagram_geoprocessing.py
Title   : HPC Instagram Geoprocessing
Author  : Ivan Ken Weng Chee 736901
Created : 08/04/2018
Purpose : COMP90024 2018S1 Assignment 1
'''

from mpi4py import MPI
import json
import numpy as np
import operator
import sys
import time

''' Obtain coordinates from Gridfile '''
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

''' Checks if a point is in a grid '''
def inside(location, coords):
    for i in range(len(coords)):
        if location['x'] >= coords[i]['xmin'] and location['x'] <= coords[i]['xmax'] and location['y'] >= coords[i]['ymin'] and location['y'] <= coords[i]['ymax']:
            return coords[i]['id']
    return None

''' Sorts and prints the final output '''
def order(posts, rows, cols):
    # Sorts the post counts
    sortedPosts = sorted(posts.items(), key=operator.itemgetter(1), reverse=True)
    sortedRows = sorted(rows.items(), key=operator.itemgetter(1), reverse=True)
    sortedCols = sorted(cols.items(), key=operator.itemgetter(1), reverse=True)

    # Final output
    print('// Rank by Unit')
    for i in range(len(sortedPosts)):
        print(sortedPosts[i][0] + ': ' + str(sortedPosts[i][1]) + ' posts')
    print('// Rank by Row')
    for i in range(len(sortedRows)):
        print(sortedRows[i][0] + '-Row: ' + str(sortedRows[i][1]) + ' posts')
    print('// Rank by Column')
    for i in range(len(sortedCols)):
        print('Column ' + sortedCols[i][0] + ': ' + str(sortedCols[i][1]) + ' posts')

''' Custom operation to reduce dictionary post counts '''
def counterSummation(x, y, datatype):
    for key in y:
        # Sum both values
        if key in x:
            x[key] += y[key]
        # Copy value over
        else:
            x[key] = y[key]
    return x

''' Main function '''
def main():

    # Checks for correct number of arguments
    if len(sys.argv) != 3:
        sys.exit()

    # Initialises MPI variables
    comm = MPI.COMM_WORLD
    size = comm.Get_size() # Number of processes
    rank = comm.Get_rank() # Process number
    name = MPI.Get_processor_name()

    # Master
    if rank == 0:
        # Starts timer
        start = time.clock()
        posts = {}
        rows = {}
        cols = {}
        coordspath = sys.argv[1]
        postspath = sys.argv[2]

        # Open coordinates
        with open(coordspath, 'r') as file:
            data = json.load(file)

        # Send coordinates to workers
        coords = getCoordinates(data)
        for i in range(1, size):
            comm.send(coords, dest=i)

        # Builds posts, rows, and columns lists
        for i in range(len(coords)):
            if coords[i]['id'] not in posts:
                posts[coords[i]['id']] = 0
            if coords[i]['id'][0] not in rows:
                rows[coords[i]['id'][0]] = 0
            if coords[i]['id'][1] not in cols:
                cols[coords[i]['id'][1]] = 0

        # Open posts
        with open(postspath, 'r') as file:

            # Multi core
            if (size > 1):
                turn = 1
                # Skips header and processes file
                next(file)
                for line in file:
                    try:
                        # Converts line into json format
                        post = json.loads(line.rstrip(", \r\n"))

                        # Finds and sends point coordinates to workers
                        if 'coordinates' in post['doc']:
                            loc = {
                                'x': post['doc']['coordinates']['coordinates'][1],
                                'y': post['doc']['coordinates']['coordinates'][0]
                            }
                            comm.send(loc, dest=turn)

                        # Change which worker to send to
                        if turn < (size - 1):
                            turn += 1
                        else:
                            turn = 1

                    except ValueError:
                        continue

                # Sends terminate signal to workers
                for i in range(1, size):
                    comm.send('terminate', dest=i)

            # Single core
            else:
                # Same process as multicore excluding MPI calls
                next(file)
                for line in file:
                    try:
                        post = json.loads(line.rstrip(", \r\n"))
                        if 'coordinates' in post['doc']:
                            loc = {
                                'x': post['doc']['coordinates']['coordinates'][1],
                                'y': post['doc']['coordinates']['coordinates'][0]
                            }
                            square = inside(loc, coords)
                            if square != None:
                                posts[square] += 1
                                rows[square[0]] += 1
                                cols[square[1]] += 1
                    except ValueError:
                        break

    # Worker
    elif rank > 0:
        # Receives coordinates from master
        coords = comm.recv(source=0)
        posts = {}
        rows = {}
        cols = {}

        # Builds posts, rows, and columns lists
        for i in range(len(coords)):
            if coords[i]['id'] not in posts:
                posts[coords[i]['id']] = 0
            if coords[i]['id'][0] not in rows:
                rows[coords[i]['id'][0]] = 0
            if coords[i]['id'][1] not in cols:
                cols[coords[i]['id'][1]] = 0

        # Keeps receiving lines from master until terminate signal is received
        while True:
            loc = comm.recv(source=0)
            if loc == 'terminate':
                break

            # Checks if points are in a grid and increment counters
            square = inside(loc, coords)
            if square != None:
                posts[square] += 1
                rows[square[0]] += 1
                cols[square[1]] += 1

    # Ensures processes are synchronised before combining results
    comm.barrier()

    # Reduces dictionary counts
    posts = comm.reduce(posts, root=0, op=MPI.Op.Create(counterSummation, commute=True))
    rows = comm.reduce(rows, root=0, op=MPI.Op.Create(counterSummation, commute=True))
    cols = comm.reduce(cols, root=0, op=MPI.Op.Create(counterSummation, commute=True))

    # Master prints final output
    if rank == 0:
        order(posts, rows, cols)

        # Prints the time taken
        end = time.clock()
        print('Time: ' + str(end - start) + 's')

if __name__ == '__main__':
    main()
