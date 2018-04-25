from db_methods import connect_to_db, drop_table, create_table
from load_movies import MovieLoader

if __name__ == '__main__':
    connection, cursor = connect_to_db()
    # create_table(cursor=cursor, table='movie_dataset')

    file_path = 'movie_dataset/movies.csv'
    table = 'movie_dataset'
    loader = MovieLoader(file_path=file_path, table=table, cursor=cursor)
    loader.load_movies()

    cursor.close()
