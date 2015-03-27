% Generate multi-dim training data for GMM. The data contains 2 Gaussians

close all; clear;

N = 100000;             % No. of training samples. Increase it if you want to use more mixtures in your GMM
D = 60;                 % Dimension of feature vectors, same size as my MFCC acoustic vectors

mu1 = -2*ones(1,D); Sigma1 = eye(D);
mu2 =  2*ones(1,D); Sigma2 = 2*eye(D);

X1 = mvnrnd(mu1, Sigma1, N);
X2 = mvnrnd(mu2, Sigma2, N);

X = [X1; X2];
P = randperm(size(X,1));
X = X(P,:);

plot(X(:,1),X(:,2),'x');

fid = fopen('input_data.txt','wt');
for i=1:size(X,1),
    for j=1:size(X,2)-1,
        fprintf(fid,'%.3f,',X(i,j));
    end
    fprintf(fid,'%.3f\n',X(i,end));
end
fclose(fid);

