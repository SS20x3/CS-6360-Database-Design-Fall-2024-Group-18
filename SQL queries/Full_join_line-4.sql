select R.src, U.dst, R.score+S.score+T.score+U.score score from graph R, graph S, graph T, graph U where R.dst=S.src and S.dst=T.src and T.dst=U.src order by score desc limit 10;
