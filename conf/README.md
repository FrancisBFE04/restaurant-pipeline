# What is this for?

This folder should be used to hold configuration files for Kedro or other utilities.

This file can be used to inform users on how to duplicate local setup using their own credentials.  You can change the file whatever you want, although you may want to keep the information below and add your own section in the area labeled **Instruction**.

## Local configuration

The 'local' folder should be used for user-specific or protected configuration (for example, security keys).

 > *Note:* Do not commit any local configurations to version control.

## Base configuration

The 'base' folder contains shared configuration, such as non-sensitive and project-related settings that can be shared across team members.

 > *WARNING:* Please do not place access credentials in the base configuration folder.
