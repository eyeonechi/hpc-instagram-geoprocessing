import json
from pprint import pprint

def main():
    data = json.load(open('data/melbGrid.json'))
    for i in range(len(data['features'])):
        pprint(data['features'][i]['geometry']['coordinates'])

if __name__ == '__main__':
    main()

'''
data
{
    crs:
    {
        properties:
        {
            name: string
        },
        type: name
    },
    features:
    [
        {
            geometry:
            {
                coordinates:
                [
                    [
                        [,],
                        [,]
                    ]
                ],
                type: Polygon
            },
            properties:
            {
                id: string,
                xmax: int,
                xmin: int,
                ymax: int,
                ymin: int
            },
            type: Feature
        },
    ]
}
'''
