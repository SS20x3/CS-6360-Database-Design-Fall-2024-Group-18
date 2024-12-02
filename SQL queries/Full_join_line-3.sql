select R.src, T.dst, R.score+S.score+T.score score from graph R, graph S, graph T where R.dst=S.src and S.dst=T.src order by score desc limit 10;
