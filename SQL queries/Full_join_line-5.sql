select R.src, V.dst, R.score+S.score+T.score+U.score+V.score score from graph R, graph S, graph T, graph U, graph V where R.dst=S.src and S.dst=T.src and T.dst=U.src and U.dst=V.src order by score desc limit 10;