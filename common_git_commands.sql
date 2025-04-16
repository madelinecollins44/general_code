--the only commands you'll probably need regularly:
git checkout -b mc/hello - makes a new branch called "mc/hello"
git checkout mc/hello - checks out an existing branch called "mc/hello"
git status shows you what's happening

--to push up changes:
make sure you're on your branch first
then git add .  - stages everything new that's on your branch
git commit -m 'your commit message here' commits all your changes to your local branch
git push - pushes your commit up to your remote branch
