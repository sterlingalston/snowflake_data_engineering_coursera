---
layout: default
title: Index
---

<ul>
{% assign doclist = site.pages | sort: 'url' %}
{% for doc in doclist %}
  {% if doc.name contains '.md' %}
    <li><a href="{{ site.baseurl }}{{ doc.url }}">{{ doc.url }}</a></li>
  {% endif %}
{% endfor %}
</ul>