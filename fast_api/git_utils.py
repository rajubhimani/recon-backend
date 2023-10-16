from github import Github, Repository


class GitUtils:
    """GitUtils
    """

    def __init__(self, access_code: str) -> None:
        """__init__

        Args:
            access_code (str): github access_code
        """
        self.github = Github(access_code)
        self.user = self.github.get_user()
        self.user.login

    def get_repo(self, repository_name: str) -> Repository.Repository:
        """_summary_

        Args:
            repository_name (str): repository_name

        Returns:
            Repository.Repository: repository object
        """
        repo = self.github.get_repo(repository_name)
        return repo

    def get_repo_file_paths(self, repository_name: str, branch_name: str) -> list:
        """get_repo_file_paths

        Args:
            repository_name (str): repository_name
            branch_name (str): branch_name

        Returns:
            list: list of paths
        """
        repo = self.get_repo(repository_name)
        contents = repo.get_contents("", ref=branch_name)
        paths = list()
        while contents:
            file_content = contents.pop(0)
            if file_content.type == "dir":
                contents.extend(repo.get_contents(
                    file_content.path, ref=branch_name))
            else:
                paths.append(file_content.path)
        return paths

    def get_file(self, repository_name: str, branch_name: str, file_path: str) -> str:
        """get_file

        Args:
            repository_name (str): repository_name
            branch_name (str): branch_name
            file_path (str): file_path

        Returns:
            str: file content
        """
        repo = self.get_repo(repository_name)
        if file_path in self.get_repo_file_paths(repository_name, branch_name):
            contents = repo.get_contents(file_path, ref=branch_name)
            response = contents.decoded_content
        else:
            response = None
        return response

    def get_files(self, repository_name: str, branch_name: str, file_path: str) -> list:
        """get_files

        Args:
            repository_name (str): repository_name
            branch_name (str): branch_name
            file_path (str): file_path

        Returns:
            list: get list of files contents
        """
        repo = self.get_repo(repository_name)
        contents = repo.get_contents(file_path, ref=branch_name)
        file_contents = dict()
        while contents:
            file_content = contents.pop(0)
            if file_content.type == "dir":
                contents.extend(repo.get_contents(file_content.path))
            else:
                file_contents[file_content.path] = file_content.decoded_content
        return file_contents

    def create_file(self, repository_name: str, branch_name: str, file_path: str, comment: str, file_content: str) -> bool:
        """create_file

        Args:
            repository_name (str): repository_name
            branch_name (str): branch_name
            file_path (str): file_path
            comment (str): comment
            file_content (str): file_content

        Returns:
            bool: status
        """
        if file_path in self.get_repo_file_paths(repository_name, branch_name):
            status = False
        else:
            repo = self.get_repo(repository_name)
            repo.create_file(file_path, comment,
                             file_content, branch=branch_name)
            status = True
        return status

    def update_file(self, repository_name: str, branch_name: str, file_path: str, comment: str, file_content: str) -> bool:
        """update_file

        Args:
            repository_name (str): repository_name
            branch_name (str): branch_name
            file_path (str): file_path
            comment (str): comment
            file_content (str): file_content

        Returns:
            bool: status
        """
        repo = self.get_repo(repository_name)
        status = False
        if file_path in self.get_repo_file_paths(repository_name, branch_name):
            contents = repo.get_contents(file_path, ref=branch_name)
            repo.update_file(contents.path, comment, file_content,
                             contents.sha, branch=branch_name)
            status = True
        return status

    def delete_file(self, repository_name: str, branch_name: str, file_path: str, comment: str) -> bool:
        """delete_file

        Args:
            repository_name (str): repository_name
            branch_name (str): branch_name
            file_path (str): file_path
            comment (str): comment

        Returns:
            bool: status
        """
        repo = self.get_repo(repository_name)
        status = False
        if file_path in self.get_repo_file_paths(repository_name, branch_name):
            contents = repo.get_contents(file_path, ref=branch_name)
            repo.delete_file(contents.path, comment,
                             contents.sha, branch=branch_name)
            status = True
        return status

    def check_path_exists(self, repository_name: str, branch_name: str, file_path: str) -> bool:
        """check_path_exists

        Args:
            repository_name (str): repository_name
            branch_name (str): branch_name
            file_path (str): file_path

        Returns:
            bool: status
        """

        all_files = self.get_repo_file_paths(repository_name, branch_name)
        return file_path in all_files
