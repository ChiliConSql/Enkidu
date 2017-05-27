#!/bin/bash

SESSION=${PWD##*/}

tmux new-session -d -s $SESSION
tmux split-window -h

tmux new-window -n 'dbutils'
tmux split-window -h
tmux send-keys -t dbutils.1 'workon falcon' C-m

tmux new-window -n 'cement'
tmux split-window -h
tmux send-keys -t cement.1 'workon falcon' C-m

tmux select-window -t $SESSION:1
tmux select-pane -t $SESSION:1.1
tmux attach-session -t $SESSION
