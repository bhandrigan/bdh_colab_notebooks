# bdh_colab_notebooks

this bash script removes versions from requirements.txt and writes the result to a new file called requirements.txt

```bash
sed 's/==.*//' requirements_with_versions.txt > requirements.txt
```

