
from bucket_join_and_agreggations import *

def test_result_aggregations():


    tuple_results=main()

    true_tuple_values=('average_most_kills_per_game_value','most_played_playlist_value','the_most_played_map_value','map_with_more_killing_spree_medals_value')

    if true_tuple_values==tuple_results:
        print("test passed")
    else:
        print("test no passed")