
# coding: utf-8

# In[111]:

import os
import struct
import numpy as np
import pandas as pd
import scipy.sparse
import scipy.optimize
from matplotlib import pyplot as plt

def load_mnist(path=r'D:\Anaconda3\codes\datasets', which='train'):
    labels_path = os.path.join(path, '%s-labels.idx1-ubyte'%which)
    images_path = os.path.join(path, '%s-images.idx3-ubyte'%which)
    with open(labels_path, 'rb') as lb_file:
        magic, n = struct.unpack('>II', lb_file.read(8))
        labels = np.fromfile(lb_file, dtype=np.uint8)
    with open(images_path, 'rb') as img_file:
        magic, num, rows, cols = struct.unpack('>IIII', img_file.read(16))
        images = np.fromfile(img_file, dtype=np.uint8).reshape((num, rows*cols))
    return labels, images

def prepare_input_data(which='train'):
    '''
    预处理数据:
    1.对像素点做归一化 参考教程是除以255 尝试过如果不做归一化 exp这个地方会溢出 而在本方法中把像素点大于1的都赋值为1
    2.按照公式应该要加上截距项 因此X应该维数加1,本方法添加了一维, 而参考教程中并没有添加一维（难道是图片影响不打吗？）
    添加了一维:Accuracy : 0.89938 未添加一维:Accuracy : 0.89666  貌似可以认为添加了一维是会提高准确率的 因为前者是包含后者的
    3.对label数据做dummy
    '''
    training_labels, training_data = load_mnist(which=which)
    
    training_data[training_data>0] = 1
#     training_data = np.hstack([training_data, np.ones((training_data.shape[0],1))])
    
    dummy = pd.get_dummies(training_labels)
    ground_truth = dummy.get_values()
    
#     相当于就是生成哑变量 不过用的是scipy的稀疏矩阵的求法(来自参考教程）， 其中data为对应行列非0位置元素值 也就是1   labels充当对应的列位置 indptr充当对应的行位置 
#     data = np.ones(len(training_labels)) # 6w data
#     indptr = np.arange(len(labels)+1)
#     ground_truth = scipy.sparse.csr_matrix((data, labels, indptr))
#     ground_truth = ground_truth.todense()
        
    return training_data, training_labels, ground_truth # n k


def show_image(images):
    '''展示图片'''
    fig, ax = plt.subplots(nrows=2, ncols=5, sharex=True, sharey=True)
    ax = ax.flatten()
    for i in range(10):
        imag = images[labels == i][0].reshape(rows, cols)
        ax[i].imshow(imag, cmap='Greys', interpolation='nearest')
    ax[0].set_xticks([])
    ax[0].set_yticks([])
    plt.tight_layout()
    plt.show()

class SoftmaxRegression():

    def __init__(self, dimension, num_classes, alpha):
        '''初始化'''
        self.dimension = dimension
        self.num_classes = num_classes
        self.alpha = alpha # weight decay parameter
        self.theta = 0.005 * np.random.randn(num_classes*dimension) #不知道这里为什么要乘以0.005 不过都是随机取数 可能经验做法
        print('初始化成功: ', 'dimension :',dimension, ' ',' num_classes :', num_classes, ' ', 'alpha :',alpha)
    
    def softmaxCost(self, theta, i, X, labels, ground_truth):
        '''
        损失函数:
        X: training_data: (N, D)
        labels: training_labels: (N, ) #为了计算错误率 不然可以省略的
        ground_truth: ground_truth of training_labels: (N,K)
        theta: 参数 (K * D) 通过优化函数 theta会被flatten 
        
        输出: 损失函数与参数
        '''
        theta = theta.reshape(self.dimension, self.num_classes)
        x_theta_exp = np.exp(X.dot(theta))  # 不对数据预处理 这里会发生了溢出
        probs = x_theta_exp/np.sum(x_theta_exp, axis=1, keepdims=True) #概率矩阵
        
        cost_ = ground_truth*np.log(probs)
        cost = -np.sum(cost_)/X.shape[0]

        theta_squared = theta*theta 
        weight_decay = 0.5 * self.alpha * np.sum(theta_squared)

        cost += weight_decay
        predictions = self.softmaxPredict(theta, X)
        error = 1 - (predictions-labels).tolist().count(0)/labels.shape[0]
        print('损失函数 :', cost, ' 错误率 :', error)
        
        #计算梯度
        theta_grad = X.T.dot(ground_truth - probs) # D K
        theta_grad = -theta_grad / X.shape[0] + self.alpha * theta

        return cost, theta_grad.flatten()

            
    def softmaxPredict(self, theta, X):
        '''
        预测函数:
        theta: 参数 
        X: 预测数据
        
        输出: 预测的标签
        '''
        theta = theta.reshape(self.dimension, self.num_classes)
        x_theta_exp = np.exp(X.dot(theta))
        probs = x_theta_exp / np.sum(x_theta_exp, axis=1, keepdims=True)

        predictions = np.argmax(probs, axis=1)
        return predictions
    
    
def executeSoftmaxRegression(training_data, training_labels, ground_truth):
    '''执行softmax'''
    dimension = training_data.shape[1]
    num_classes = 10 # len(set(training_labels))
    alpha = 0.001
    max_iterations = 500
    
    regressor = SoftmaxRegression(dimension, num_classes, alpha)
    
    opt_solution = scipy.optimize.minimize(regressor.softmaxCost, regressor.theta, 
                                    args = (0, training_data, training_labels, ground_truth), method = 'L-BFGS-B', 
                                    jac = True, options = {'maxiter': max_iterations})
    return opt_solution, regressor

def test(theta, regressor):
    """ 在测试集上进行测试 """
    test_data, test_labels, _ = prepare_input_data(which='test')   

    predictions = regressor.softmaxPredict(theta, test_data)
  
    correct = test_labels == predictions # 同理也可以算错误率
    print("测试集准确率 :", numpy.mean(correct))

if __name__ == '__main__':
    training_data, training_labels, ground_truth = prepare_input_data(which='train')
    opt_solution, regressor = executeSoftmaxRegression( training_data, training_labels, ground_truth )
    print('opt_solution: ', opt_solution)
    test(opt_solution.x, regressor)

