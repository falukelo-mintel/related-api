git clone https://$GIT_USER:$GIT_KEY@github.com/$GIT_USER/$GIT_REPO --branch=$GIT_BRANCH_REL $GIT_BRANCH_REL
cp data/df_associations.csv $GIT_BRANCH_REL/app/data/
cd $GIT_BRANCH_REL/
git remote set-url origin https://$GIT_USER:$GIT_KEY@github.com/$GIT_USER/$GIT_REPO
git add app/data/df_associations.csv 
git commit -m 'update association rule'
git push origin $GIT_BRANCH_REL --force
rm -rf $GIT_BRANCH_REL/
