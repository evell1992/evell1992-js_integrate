triangle = [
     [2],
    [3,4],
   [6,5,7],
  [4,1,8,3]
]
_all = triangle[0]
for i in range(1, len(triangle)):
    tmp = []
    for index, _ in enumerate(triangle[i]):
        if index == 0:
            triangle[i][index] = _+triangle[i-1][index]
        elif index == len(_all):
            triangle[i][index] = _ + triangle[i - 1][index-1]
        else:
            triangle[i][index] = _ + triangle[i - 1][index]
            tmp.append(min(_+triangle[i - 1][index-1],_+triangle[i - 1][index]))
return min(triangle[len(triangle)-1])