data = read.csv('cpu_users_arc2.txt',stringsAsFactors=FALSE,sep=' ')
print('arc2')
#How many seconds has the top user used
top = max(data[,1])
# How many seconds have the top 10 users used?
top_10 = sum(tail(data[,1],10))

print(paste('There are ',length(data[,1]),' users'),sep='')

#How many users does it take to exceed time used by top user?
users_below_top = min(which( top-cumsum(data[,1]) < 0))
print(paste('Top user has used more than the bottom ', users_below_top,' combined'),sep='')

#How many users does it take to exceed time used by top 10?
users_below_top_10 = min(which( top_10-cumsum(data[,1]) < 0))
print(paste('Top 10 users have used more than the bottom ', users_below_top_10,' combined'),sep='')

