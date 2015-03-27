% Generate sales figures for EIE4108 MapReduce example

close all; clear;

N = 10000;     % No. of customers (lines)
M = 12;         % No. of months

salesMat = zeros(N,M);
for i=1:N,
    salesMat(i,:) = randi(50,1,M);
end

fid = fopen('sales_figure.txt','wt');
for i=1:N,
    fprintf(fid,'%d,',i);
    for j=1:M-1,
        fprintf(fid,'%d,',salesMat(i,j));
    end
    fprintf(fid,'%d\n',salesMat(i,end));
end
fclose(fid);