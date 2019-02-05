import matplotlib.pyplot as plt

file = open('000000_0', 'r')

x = []
y = []

for line in file:
    split = line.split(',')
    x.append(int(split[0][6:]))
    if split[1] != '\\N\n':
        y.append(float(split[1]))
    else:
        y.append(0.0)



plt.plot(x,y)
plt.show()
