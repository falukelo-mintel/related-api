git clone https://$GIT_USER:$GIT_KEY@github.com/$GIT_USER/$GIT_REPO --branch=$GIT_BRANCH_REC $GIT_BRANCH_REC
cp data/recommended.csv $GIT_BRANCH_REC/app/data/
cp data/cossim_matrix.csv $GIT_BRANCH_REC/app/data/
cd $GIT_BRANCH_REC/
git remote set-url origin https://$GIT_USER:$GIT_KEY@github.com/$GIT_USER/$GIT_REPO
git add app/data/recommended.csv 
git add app/data/cossim_matrix.csv 
git commit -m 'update recommendation rule'
git push origin $GIT_BRANCH_REC --force
rm -rf $GIT_BRANCH_REC/
