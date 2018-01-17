
# coding: utf-8

# In[29]:

import pandas as pd
import statsmodels.api as sm
import numpy as np
import matplotlib.pyplot as plt
import scipy.optimize


def poisson_regression_cost(theta, X, labels):
    '''泊松回归损失函数以及梯度推导'''
    theta = theta.reshape(X.shape[1],1)
    x_theta = X.dot(theta) # n 1
    cost = -np.sum(labels*x_theta-np.exp(x_theta))
#     print('cost: ', cost)
    theta_grad = np.sum(np.exp(x_theta)*X, axis=0)-labels.T.dot(X)
    return cost, theta_grad.flatten()

def load_data(path):
    data = pd.read_csv(path, delimiter=',',header=0)
    return data

def show_hist(data):
    # 画图查看是否符合泊松分布
    histData = []
    uniqProgs = sorted(data['prog'].unique())
    for elem in uniqProgs:
        histData.append(data[data['prog'] == elem]['num_awards'].values)
    plt.hist(tuple(histData),bins=10, normed=True,histtype='bar',label= map(lambda x: 'Prog '+ str(x),uniqProgs))
    plt.legend()
    plt.ylabel('Count')
    plt.title('Histogram for each program')
    plt.show()

def prepare_data(data):
    # 本函数中prog是类别 需要做dummy
    prog_dummies = pd.get_dummies(data['prog']).rename(columns=lambda x: 'prog_' + str(x))
    dataWithDummies = pd.concat([data, prog_dummies], axis=1) 
    dataWithDummies.drop(['prog', 'prog_3'], inplace=True, axis=1)# 主要是共线问题删除一个变量
    dataWithDummies = dataWithDummies .applymap(np.int)
    
    feat_cols = ['math', 'prog_1', 'prog_2'] # X_features
    X = sm.add_constant(dataWithDummies[feat_cols].values, prepend=False) #加上截距项
    Y = dataWithDummies['num_awards'].values
    return X, Y 

path = r'D:\Anaconda3\codes\datasets\StudentData.csv'
data = load_data(path)
X, Y = prepare_data(data)

# 自带model做出来的结果
poisson_mod = sm.Poisson(Y, X)
poisson_res = poisson_mod.fit(method="newton")
print(poisson_res.summary())

# 自己编写的泊松回归
theta = np.random.randn(X.shape[1],1)
max_iterations = 500
Y_ = Y.reshape(X.shape[0],1)
opt_solution = scipy.optimize.minimize(poisson_regression_cost, theta, 
                                    args=( X, Y_,), method = 'L-BFGS-B', 
                             jac = True, options = {'maxiter': max_iterations})
print(opt_solution)

# reference: https://github.com/mahat/PoissonRegression/blob/master/SimplePoissonRegression.py