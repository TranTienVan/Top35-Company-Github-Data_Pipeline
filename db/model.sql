create database github_profile2;
use github_profile2;

CREATE TABLE `USER`
            (
                id bigint,
                login varchar(200),
                html_url varchar(400) unique,
                type varchar(200),
                name varchar(400),
                blog varchar(400),
                location varchar(400),
                email varchar(400) unique,
                public_repos int,
                followers int,
                following int,
                created_at timestamp,
                updated_at timestamp,
                
                primary key (id)
            );

CREATE TABLE `ORGANIZATION_MEMBER`
            (
                organization_id bigint,
                user_id bigint,
                
                primary key (organization_id, user_id),
                foreign key (organization_id) references `USER`(id),
                foreign key (user_id) references `USER`(id)
            );


CREATE TABLE `LICENSE`(
                `key` varchar(200),
                name varchar(200),
                spdx_id varchar(200),
                description text,
                implementation text,
                html_url varchar(200),
                
                primary key (`key`)
            );

CREATE TABLE `REPOSITORY`
            (
                id bigint,
                name varchar(200),
                full_name varchar(400),
                html_url varchar(400),
                description text,
                created_at timestamp,
                updated_at timestamp,
                pushed_at timestamp,
                homepage varchar(400),
                size int,
                stargazers_count int,
                watchers_count int,
                forks_count int,
                open_issues_count int,
                license_key varchar(200),
                organization_id bigint not null,
                
                primary key (id),
                foreign key (license_key) references `LICENSE`(`key`),
                foreign key (organization_id) references `USER`(id)
            );
	
CREATE TABLE `COMMIT`(
                sha varchar(200),
                date timestamp,
                message text,
                comment_count int,
                html_url varchar(200),
                author_id bigint not null,
                repository_id bigint not null,
                
                primary key (sha),
                foreign key (author_id) references `USER`(id),
                foreign key (repository_id) references `REPOSITORY`(id)
            );

CREATE TABLE `REPOSITORY_TAG`(
                commit_sha varchar(200),
                name varchar(200),
                zipball_url varchar(200),
                
                primary key (commit_sha, name),
                foreign key (commit_sha) references `COMMIT`(sha)
            );

CREATE TABLE `REPOSITORY_LANGUAGE`(
                repository_id bigint,
                language varchar(200),
                used_count int,
                
                primary key (repository_id, language),
                foreign key (repository_id) references `REPOSITORY`(id)
            );

CREATE TABLE `REPOSITORY_CONTRIBUTOR`(
                repository_id bigint,
                contributor_id bigint,
                contributions int,
                
                primary key (repository_id, contributor_id),
                foreign key (repository_id) references `REPOSITORY`(id),
                foreign key (contributor_id) references `USER`(id)
            );

CREATE TABLE `REPOSITORY_ACTIVITY`(
                repository_id bigint,
                datetime_updated timestamp,
                
                primary key (repository_id, datetime_updated),
                foreign key (repository_id) references `REPOSITORY`(id)
            );