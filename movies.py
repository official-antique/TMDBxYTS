from datetime import datetime, timedelta
from pymongo import MongoClient
import concurrent.futures, gzip, json, time, urllib.error, urllib.request

mongodb_username = ''
mongodb_password = ''

tmdb_api_key = ''




client = MongoClient('localhost', username=mongodb_username, password=mongodb_password)
database = client.quix.movies

class TMDBMovie():
        def addWithKeyPath(self, key, key_path, data):
                setattr(self, key, find(key_path, data))

        def addWithKey(self, key, data, using_dict):
                setattr(self, key, data[key] if using_dict else data)
                

def find(element, json):
    keys = element.split('.')
    rv = json
    for key in keys:
        rv = rv[key]
    return rv


def gunzip(file):
        with gzip.GzipFile(fileobj=file) as decompressed:
                return decompressed.read()

def download_movie_ids_gzip():
        today = datetime.today() - timedelta(days=1)
        current_date = today.strftime('%m_%d_%Y')
        try:
                with urllib.request.urlopen(f'https://files.tmdb.org/p/exports/movie_ids_{current_date}.json.gz') as response:
                        json_file = gunzip(response)
                with open('/home/ubuntu/quix/movies.json', 'wb') as output:
                        output.write(json_file)
                        print('successfully saved file')
                        return 0
        except:
                print('failed to download file')
                return 1


def convert_str_to_dict(string):
        return json.loads(string)

def get_ids_from_movies_json():
        ids = []
        with open('/home/ubuntu/quix/movies.json') as json_file:
                for line in json_file.readlines():
                        id = convert_str_to_dict(line)['id']
                        ids.append(id)
                return ids



def tmdb(id):
        request = urllib.request.Request(f'https://api.themoviedb.org/3/movie/{id}?api_key={tmdb_api_key}&append_to_response=translations,credits,images,keywords,videos,recommendations,reviews')
        request.add_header('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:96.0) Gecko/20100101 Firefox/96.0')
        try:
                with urllib.request.urlopen(request, timeout=10) as url:
                        data = json.loads(url.read().decode())
                        return data
        except urllib.error.HTTPError as e:
                return None


def now_playing():
        request = urllib.request.Request(f'https://api.themoviedb.org/3/movie/now_playing?api_key={tmdb_api_key}')
        request.add_header('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:96.0) Gecko/20100101 Firefox/96.0')
        try:
                with urllib.request.urlopen(request, timeout=10) as url:
                        data = json.loads(url.read().decode())
                        return data['results']
        except urllib.error.HTTPError as e:
                return None


def tmdb_companies(id):
        request = urllib.request.Request(f'https://api.themoviedb.org/3/company/{id}?api_key={tmdb_api_key}')
        request.add_header('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:96.0) Gecko/20100101 Firefox/96.0')
        try:
                with urllib.request.urlopen(request, timeout=10) as url:
                        data = json.loads(url.read().decode())
                        return data
        except urllib.error.HTTPError as e:
                return None


def yts(imdb_id):
        request = urllib.request.Request(f'https://yts.mx/api/v2/list_movies.json?query_term={imdb_id}')
        request.add_header('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:96.0) Gecko/20100101 Firefox/96.0')
        try:
                with urllib.request.urlopen(request, timeout=10) as url:
                        data = json.loads(url.read().decode())
                        return data
        except urllib.error.HTTPError as e:
                return None


def get_torrents(movie):
        yts_data = yts(movie.imdb_id)
        if yts_data is not None and yts_data['data']['movie_count'] > 0 and 'movies' in yts_data['data'] and 'torrents' in yts_data['data']['movies'][0]:
                torrents = yts_data['data']['movies'][0]['torrents']
                for torrent in torrents: torrent['source'] = 'yts'
                movie.addWithKey('torrents', torrents, False)
                return movie
        else:
                return movie


def get_complete_data_from_id(id):
        print(f'{ids.index(id)} of {len(ids)}')
        data_keys = ['backdrop_path', 'budget', 'credits.cast', 'credits.crew', 'genres', 'homepage', 'id', 'imdb_id', 'overview', 'poster_path', 'production_companies', 'production_countries', 'recommendations.results', 'release_date', 'revenue', 'reviews.results', 'runtime', 'status', 'tagline', 'title', 'translations.translations', 'videos.results']
        movie_keys = ['backdrop_path', 'budget', 'cast', 'crew', 'genres', 'homepage', 'id', 'imdb_id', 'overview', 'poster_path', 'production_companies', 'production_countries', 'recommendations', 'release_date', 'revenue', 'reviews', 'runtime', 'status', 'tagline', 'title', 'translations', 'videos']

        tmdb_data = tmdb(id)
        if tmdb_data is not None:
                movie = TMDBMovie()
                for index, key in enumerate(data_keys):
                        movie.addWithKey(key, tmdb_data, True) if '.' not in key else movie.addWithKeyPath(movie_keys[index], key, tmdb_data)

                

                if hasattr(movie, 'imdb_id') and movie.imdb_id is not None:
                        torrent_movie = get_torrents(movie)
                        torrent_movie.addWithKey('is_recommendation', True if torrent_movie.id in now_playing_ids else False, False)
                        
                        if datetime.strptime(torrent_movie.release_date, '%Y-%m-%d').year >= (datetime.today().year - 10):
                                upload(torrent_movie)
                                time.sleep(0.25)
                        else:
                                pass
                else:
                        pass
        else:
                pass



def upload(movie):
        if database.find_one({ 'imdb_id' : movie.imdb_id }):
                database.find_one_and_replace({ 'imdb_id' : movie.imdb_id }, vars(movie))
        else:
                database.insert_one(vars(movie))


now_playing_ids = []
now_playing_func = now_playing()
if now_playing_func:
        for result in now_playing_func:
                now_playing_ids.append(result['id'])

        return_code = download_movie_ids_gzip()
        ids = get_ids_from_movies_json()
        if return_code == 0:
                with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
                        executor.map(get_complete_data_from_id, ids)
        else:
                print('failed')

        print('done', datetime.now().strftime('%A:%b %H:%M:%S'))