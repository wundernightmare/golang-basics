package testx

import (
	allure "github.com/ozontech/testo-allure"
	"github.com/ozontech/testo/testoplugin"
)

// Meta is the TestOps identity of a suite: where it sits in the Epic /
// Feature tree and who answers for it. Per-test identity (the case ID, the
// Story, links to issues) is set inside the test with t.ID / t.Story /
// t.Links, so the report can be filtered by both.
//
// SAMPLE VALUES. This repository is a template: the Epic, Feature, Owner and
// the "GB-<n>" IDs used across the suites are placeholders whose *shape* is
// the point — replace them with your Allure TestOps project's tree, owner
// handles and case IDs. One rule survives the replacement: an ID appears in
// exactly one test, and a test without an ID is not in the test plan.
type Meta struct {
	Epic    string // top-level grouping, usually the product or service line ("golang-basics")
	Feature string // the capability under test ("tasks API", "tasks resilience")
	Owner   string // team or person handle as your TestOps knows it ("@team-platform")
}

func (m Meta) options() []testoplugin.Option {
	var opts []testoplugin.Option
	var labels []allure.Label
	if m.Epic != "" {
		labels = append(labels, allure.NewLabel("epic", m.Epic))
	}
	if m.Feature != "" {
		labels = append(labels, allure.NewLabel("feature", m.Feature))
	}
	if len(labels) > 0 {
		opts = append(opts, allure.WithLabels(labels...))
	}
	if m.Owner != "" {
		opts = append(opts, allure.WithOwner(m.Owner))
	}
	return opts
}

// LinkTransformer turns bare IDs into links for the report: a TMS id
// ("GB-101") into the TestOps case, an issue id into the tracker. Sample
// hosts — point them at yours.
func LinkTransformer(link allure.Link) allure.Link {
	switch link.Type {
	case allure.LinkTypeTMS:
		link.URL = "https://testops.example.internal/project/1/test-cases/" + link.URL
	case allure.LinkTypeIssue:
		link.URL = "https://issues.example.internal/browse/" + link.URL
	}
	return link
}

// Case binds a test to its TestOps case: the id (as the Allure ID, which is
// what test plans and history key on) plus a TMS link through
// [LinkTransformer], and the Story it belongs to. One id per test.
//
//	testx.Case(t, "GB-101", "create, read, list, delete a task")
func Case(t T, id, story string) {
	t.ID(id)
	t.Story(story)
	t.Links(allure.TMS(id))
}
