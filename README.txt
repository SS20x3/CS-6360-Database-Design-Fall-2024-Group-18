A DBMS or some way to run SQL queries is necessary, our group use the DuckDB cli found here:
https://duckdb.org/docs/installation/?version=stable&environment=cplusplus&platform=win&download_method=direct&architecture=x86_64

The python files can be run in cli in the format python [program].py [path to file] [k] [line].

examples:
python.exe .\product_k.py ..\Datasets\deezer_europe_edges_with_scores.csv 20 4
python .\level_k.py ..\Datasets\facebook_combined_with_scores.csv 10 3

The SQL queries can be run in a DBMS that contains a table called 'graph' with the fields 'src', 'dst', and 'score'.
This table can be generated in DuckDB with the command:
	Create table graph as (select column0 src, column1 dst, column2 score FROM '[fileName]')