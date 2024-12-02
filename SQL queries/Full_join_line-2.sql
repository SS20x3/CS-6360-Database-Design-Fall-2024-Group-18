select R.src, S.dst, R.score+S.score score from graph R, graph S where R.dst=S.src order by score desc limit 10;
