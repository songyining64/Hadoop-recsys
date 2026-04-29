MovieLens 100K — place unpacked files here (or any directory and set ML100K_DIR).

Required files (names are case-insensitive; subfolders are searched):
  u.data   ratings (user id, item id, rating, timestamp)
  u.item   movies + genre flags
  u.user   demographics

Official: https://grouplens.org/datasets/movielens/100k/
Mirror: https://www.kaggle.com/datasets/prajitdatta/movielens-100k-dataset

After unzip you may have ml-100k/u.data etc.; either move *.data *.item *.user into this folder or point ML100K_DIR to the unzip root.

Run full pipeline (from project root):
  bash pipeline/run_all_docker.sh

Custom data path:
  ML100K_DIR=/path/to/ml-100k bash pipeline/run_all_docker.sh
