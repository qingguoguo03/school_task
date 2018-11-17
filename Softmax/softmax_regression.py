from sklearn import datasets
digits = datasets.load_digits()
X_RAW = digits.images
Y_RAW = digits.target
# plt.imshow(X[19], cmap=plt.cm.gray_r, interpolation='nearest')
# plt.show()
# 对x做下拉平
def data_process():
    X = X_RAW.copy()
    Y = Y_RAW.copy()
    X[X>0] = 1 # 0 1 处理
    X = np.hstack((X,np.ones((n_examples,1))))
    images = []
    for image in X:
        images.append(image.flatten())
    X = np.array(images)
    dummy = pd.get_dummies(Y)
    Y = dummy.astype('int8').get_values()
    n_examples = int(X.shape[0]*(9/10)) # 十折交叉验证
    random_index = np.random.randint(0, X.shape[0], n_examples)
    X_trains = X[random_index]
    Y_trains = Y[random_index]
    Y_trains_label = digits.target[random_index] #后续会用到labels
    indexs = list(set(range(X.shape[0]))-set(random_index))
    X_test = X[indexs]
    Y_test = Y[indexs]
    Y_test_label = digits.target[indexs]
    return X_trains, Y_trains, Y_trains_label, X_test, Y_test, Y_test_label

def train(X, Y, Y_, iteration):
    epsilon = 0.01
    n_examples = X.shape[0]
    Y_predict = np.zeros(shape=Y_.shape)
    theta = np.random.randn(X.shape[1], Y.shape[1]) # (65, 10)
    
    for i in range(iteration):
        z = X.dot(theta)
        h = np.exp(z)/np.sum(np.exp(z), axis=1, keepdims=True)
        delta = X.T.dot(Y - h*Y)  #这里没有搞懂感觉与公式不大一样 改了效果并不是很好
        theta += epsilon*delta/n_examples

        z1 = X.dot(theta)
        h1 = np.exp(z1)/np.sum(np.exp(z1), axis=1, keepdims=True)
        probs = np.sum(np.log(h1)*Y)
        loss = -probs/n_examples

        for j in range(n_examples):
            Y_predict[j] = np.argmax(h[j,:])
        error = 1-(Y_predict - Y_).tolist().count(0)/n_examples
        if i % 100 == 0:
            print('Iteration %d with loss = %f and Error rate = %f'%(i, loss, error))
    return theta
    

def test(theta, X, Y_):
    z = X.dot(theta)
    h = np.exp(z)/np.sum(np.exp(z), axis=1, keepdims=True)
    Y_predict = np.zeros(shape=Y_.shape)
    for i in range(X.shape[0]):
        Y_predict[i] = np.argmax(h[i,:])
    error = 1 - (Y_predict - Y_).tolist().count(0)/X.shape[0]
    print('In test datasets: error rate is %f'%(error))


X_trains, Y_trains, Y_trains_label, X_test, Y_test, Y_test_label = data_process()
theta = train(X_trains, Y_trains, Y_trains_label, 19000)  #  0.086580
test(theta, X_test, Y_test_label)

